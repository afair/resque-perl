package Resque::Worker;

use 5.006000;
use strict;
use warnings;

use JSON;
use Resque::Job;

require Exporter;

our @ISA = qw(Exporter Resque);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use Resque ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	
);

our $VERSION = '0.01';
our $RETRY_EXIT_CODE = 111;

my $hostname;
my %workers=();
my $to_s;

use Data::Dumper;
sub new {
  my ($class, %config) = @_;
  if (exists($config{queues}) && ref($config{queues}) ne 'ARRAY') {
    $config{queues} = [split(/[\s\,]/, $config{queues}||$ENV{QUEUES}||'*')];
  }
  my $self = {verbose=>0, 
    queues=>[$ENV{QUEUES}||'*'], 
    state=>'idle', %config};
  $self = $config{resque} ? {%{$config{resque}},%$self} : new Resque(%$self);
  bless $self, $class;
  $self->register_worker();
  $self;
}

# Removes a non-processing worker data
sub destroy {
  my ($self) = @_;
  $self->unregister_worker();
}

sub hostname {
  $hostname ||= `hostname`;
  chomp $hostname;
  $hostname;
}

# Names the worker: hostname:pid:queue,queue,....
sub to_s {
  my ($self) = @_;
  $to_s ||= join(":", hostname(), $$, join(',',@{$self->{queues}}));
}

sub set {
  my ($self, $key, $value) = @_;
  $self->redis->set($self->key($key, $self->to_s), $value);
}

sub increment {
  my ($self, $key, $by) = @_;
  $self->redis->incrby($self->key($self->to_s(), $key), $by||1);
}

# Sets up data structures for worker:
#   base:workers                                 set(worker names)
#   base:processed                               0 
#   base:worker:hostname:pid:queue,queue:started time
#   base:worker:hostname:pid:queue,queue         current job (set when job selected)
sub register_worker {
  my ($self) = @_;
  $self->prune_dead_workers();
  $workers{$self->to_s} = $self;
  $self->redis->sadd($self->key('workers'), $self->to_s);
  $self->set('processed', '0');
  $self->set('failures',  '0');
  $self->set('started', $self->now());
}

sub unregister_worker {
  my ($self) = @_;
  delete $workers{$self->to_s};
  $self->drop_keys('worker', $self->to_s);
  $self->redis->del($self->key('processed', $self->to_s));
  $self->redis->srem($self->key('workers'), $self->to_s);
}

# Returns the next job from the subscribed queues
sub reserve {
  my ($self) = @_;
  #my @q = $self->{queues} eq '*' ? sort $self->queues() : split(',',@{$self->{queues}});
  foreach (@{$self->{queues}}) {
    $self->{job} = Resque::Job::reserve($self, $_);
    return $self->{job} if $self->{job};
  }
}

# Returns an array of all known workers. Call with resque base class or subclass
sub workers {
  my ($resque) = @_;
  $resque->redis->smembers($resque->key("workers"));
}

sub prune_dead_workers {
  my ($self) = @_;
  my @rworkers = $self->workers();
  foreach my $w (@rworkers) {
    my ($pid) = $w =~ /:(\d+):/;
    my $wps = `ps -o pid,state -p $pid`;
    $self->redis->srem($self->key('workers'), $w) unless $wps;
    $self->redis->del($self->key('worker', $w, 'started'));
    $self->redis->del($self->key('processed', $w));
  }
}

# Increments the named status count for the worker and system
sub status {
  my ($self, $name) = @_;
  $self->redis->incrby($self->key('stat', $name), 1);
  $self->redis->incrby($self->key('stat', $self->to_s, $name), 1);
}

# Signals: QUIT=exit, TERM=kill child & exit, USR1=kill child & continue, USR2=Pause, CONT=Unpause
sub register_signal_handlers {
  my ($self) = @_;
  $self->{paused} = 0;
  $self->{shutdown} = 0;
  $self->{died} = 0;
  $SIG{INT}  = sub { $self->shutdown_now("Interrupted") };
  $SIG{TERM} = sub { $self->shutdown_now("Caught TERM") };
  $SIG{QUIT} = sub { $self->shutdown("Caught QUIT") };
  $SIG{USR1} = sub { $self->kill_child("Caught USR1") };
  $SIG{USR2} = sub { $self->pause_processing("Caught USR2") };
  $SIG{CONT} = sub { $self->unpause_processing("Caught CONT") };
#   $SIG{__DIE__} = sub { 
#     return if $self->{died};
#     if ($self->{child} && $self->{child} == $$) { # I'm a child
#       ++$self->{died};
#       $self->logger("Child died...",  @_);
#       exit 255;
#     }
#     else { # Master died. WTF?
#       $self->logger("Master died...",  @_);
#       exit 255;
#     }
#   };
}

sub unregister_signal_handlers {
  my ($self) = @_;
  delete $SIG{INT};
  delete $SIG{TERM};
  delete $SIG{QUIT};
  delete $SIG{USR1};
  delete $SIG{USR2};
  delete $SIG{CONT};
 #delete $SIG{__DIE__};
}

sub work {
  my ($self) = @_;
  $self->redis->sadd($self->key('workers'), $self->to_s);
  $self->redis->set($self->key('stat', 'processed', $self->to_s), "0");
  $self->register_signal_handlers();
  $self->check_cron();

  while(!$self->shutdown_requested) {
    if (!$self->paused && $self->reserve()) {
      my $rc = 0;
      $self->working_on();

      if ($self->{no_fork}) {
        $rc = $self->execute_job();
      }
      else {
        $self->{child} = fork();
        $self->shutdown("Fork failed!") unless defined $self->{child};

        if ($self->{child} == 0) { # Child!
          srand;
          $rc = $self->execute_job();
          exit ($rc ? $rc : 0);
        }
        else { # Me, $self->{child} is the child pid
          my $cpid = wait();
          #print "*** CHILD COMPLETE $cpid, $?\n";
          $self->{child_exit} = $?;
          if ($? == $RETRY_EXIT_CODE) {
            $self->fail_job("Retry Requested $?");
          }
          elsif ($? > 1) {
            $self->fail_job("Unknown Exit Error $?");
          } 
          elsif ($cpid == -1) {
            $self->logger("Lost my child! Sad Parent!") if $cpid == -1;
          }
        }
        $self->{child} = 0;
        $self->done_working();
      }
    }
    elsif ($self->{shutdown_on_empty}) {
      #print "===== EMPTY\n";
      $self->shutdown();
    } 
    else {
      #print "===== SLEEP\n";
      sleep($self->{interval} || 5);
      $self->check_cron();
    }
  }
}


sub execute_job {
  my ($self) = @_;
  my $rc = 0;
  # Send job to a configired callback, or let the job do it.
  if ($self->{callback}) {
    $rc = &{$self->{callback}}($self->{job}->args());
  } elsif ($self->{callback_with_job}) {
    $rc = &{$self->{callback_with_job}}($self->{job}, $self->{job}->args());
  } else {
    $rc = $self->{job}->perform();
  }
  $self->status('processed');
  $rc;
}

# Log Process Worker: reads base:logs:logname and writes lines to given logname
sub log_processor {
  my ($self) = @_;
  $self->redis->sadd($self->key('workers'), $self->to_s);
  $self->redis->set($self->key('stat', 'processed', $self->to_s), "0");
  $self->register_signal_handlers();
  
  while(!$self->shutdown_requested) {
    my @logs = $self->available_logs();
    if (!$self->paused && scalar(@logs)) {
      my $clog = '';
      my ($lh, $rec);
      foreach my $l (@logs) {
        my ($lname) = $l =~ /^.+?:log:(.+)/;
        while (!$self->paused && ($rec = $self->pop_log_rec($l))) {
          if ($clog ne $lname) {
            $clog = $lname;
            close $lh if $lh;
            open $lh, '>>', $lname;
          }
          print $lh "$rec\n" if $lh;
        }
        close $lh if $lh;
      }
    }
    elsif ($self->{shutdown_on_empty}) {
      $self->shutdown();
    } 
    else {
      sleep($self->{interval} || 5);
    }
  }
  $self->redis->srem($self->key('workers'), $self->to_s);
  $self->redis->del($self->key('stat', 'processed', $self->to_s));
}

# Updates base:worker:id to current job
sub working_on {
  my ($self, $job) = @_;
  $job ||= $self->{job};
  my $v = {queue=>$self->{queue}, run_at=>Resque::now(), payload=>$job->{payload}};
  $self->redis->set($self->key('worker',$self->to_s), encode_json($v));
}

sub done_working {
  my ($self) = @_;
  $self->redis->del($self->key('worker',$self->to_s));
}

# Call when job failed.
sub fail_job {
  my ($self, $exception) = @_;
  my $job ||= $self->{job};
  my $v = JSON::encode_json({%$job, queue=>$self->{queue}, run_at=>Resque::now(),
    exception=>$exception, worker=>$self->to_s, failed_at=>Resque::now()});
  $self->increment('failures');
  $self->redis->incrby($self->key("stat:failed:$self->{worker}"), 1);
  $self->redis->incrby($self->key("stat:failed"), 1);
}

sub retry_queue {
  my ($self, $queue) = @_;
  while (my $obj = $self->dequeue_job($self->queue($self->{queue}))) {
    $self->queue_job($queue, $obj);
  }
}

# Finishes current job and halts
sub pause_processing {
  my ($self, @msg) = @_;
  $self->logger("Pausing...", @msg);
  $self->{paused} = time;
}

# Finishes current job and halts
sub unpause_processing {
  my $self = shift;
  $self->logger("Continuing...");
  $self->{paused} = 0;
}

# Finishes current job and halts
sub shutdown {
  my ($self, @msg) = @_;
  $self->logger("Exiting...", @msg);
  $self->{shutdown} = time;
}

# Cancels current job and halts
sub shutdown_now {
  my ($self, @msg) = @_;
  $self->kill_child(@msg);
  $self->shutdown(@msg);
}

# Finishes current job and halts
sub shutdown_requested {
  $_[0]->{shutdown} ? 1 : 0;
}

# Finishes current job and halts
sub paused {
  $_[0]->{paused} ? 1 : 0;
}

sub kill_child {
  my ($self, @msg) = @_;
  my $info = $self->child_process_info() or return 0;
  $self->logger("Killing child at $self->{child}", $info, @msg);
  `kill -9 $self->{child}`;
}

sub child_process_info {
  my $self = shift;
  return '' unless $self->{child};
  my $info = `ps -o pid,state -p $self->{child}`;
  return undef if $?; # Not found
  chomp $info;
  $info;
}

# Verbose logger
sub vlog {
  my ($self, @msg) = @_;
  $self->logger(@msg) if $self->{verbose};
}

# Appends message to log file
sub logger {
  my ($self, @msg) = @_;
  Resque::logger($self, $ENV{WORKER_LOGFILE}||$self->to_s(), @msg);
}


1; # End of Package
