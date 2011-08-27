package Resque::Worker;

use 5.006000;
use strict;
use warnings;

use Redis;
use JSON::XS;

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

my $hostname;
my %workers={};
my $to_s;

sub new {
  my ($class, %config) = @_;
  my $self = new Resque(
    verbose=>0, queues=>$ENV{QUEUES}||'*', state=>'idle',
    %config);
  bless $self, $class;
  $self->register_worker();
  $self;
}

sub hostname {
  $hostname ||= `hostname`;
  chomp $hostname;
  $hostname;
}

sub to_s {
  my ($self) = @_;
  $to_s ||= join(":", hostname(), $$, join(',',$self->{queues}));
}

sub register_worker {
  my ($self) = @_;
  $workers{$self->to_s} = $self;
  $self->redis->sadd($self->resque->key($self->to_s));
}

sub unregister_worker {
  my ($self) = @_;
  delete $workers{$self->to_s};
  $self->redis->sadd($self->resque->key($self->to_s));
}

# Returns the next job from the subscribed queues
sub reserve {
  my ($self) = @_;
  my @q = $self->{queues} eq '*' ? sort $self->queues() : split(',',$self->{queues});
  foreach (@q) {
    $self->{job} = $self->pop_job($_);
    if ($self->{job}) {
      $self->{payload} = encode_json($self->{job});
      $self->{reserve_queue} = $_;
      return $self->{job};
    }
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
  }
}

# Increments the named status count for the worker and system
sub status {
  my ($self, $name) = @_;
  $self->redis->incby($self->key('stat', $name));
  $self->redis->incby($self->key('stat', $name, $self->to_s));
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
  $SIG{__DIE__} = sub { 
    return if $self->{died};
    if ($self->{child} && $self->{child} == $$) { # I'm a child
      ++$self->{died};
      $self->logger("Child died...",  @_);
      exit 255;
    }
    else { # Master died. WTF?
      $self->logger("Master died...",  @_);
      exit 255;
    }
  };
}

sub worker {
  my ($self, $queues, $callback) = @_;
  $self->redis->sadd($self->key('workers'), $self->to_s);
  $self->redis->set($self->key('stat', 'processed', $self->to_s), "0");
  $self->register_signal_handlers();

  while(!$self->shutdown_requested) {
    if (!$self->paused && $self->reserve()) {
      $self->working_on();
      $self->{child} = fork();
      $self->shutdown("Fork failed!") unless $self->{child};

      if ($self->{child} == 0) { # Child!
        srand;
        my $rc = &$callback(@{$self->{job}{args}});
        $self->status('processed');
        exit $rc;
      } 
      else { # Me, $self->{child} is the child pid
        my $cpid = wait();
        blwarn("Lost my child! Sad Master") if $cpid == -1;
      }
      $self->{child} = 0;
      $self->done_working();
    } 
    else {
      sleep($self->{interval} || 5);
    }
  }
  $self->redis->srem($self->key('workers'), $self->to_s);
  $self->redis->del($self->key('stat', 'processed', $self->to_s));
}

sub working_on {
  my ($self, $job) = @_;
  $job ||= $self->{job};
  my $v = encode_json({queue=>$self->{queue}, run_at=>BmUtil::currentTimestamp(),
    payload=>$job});
  $self->redis->set("resque:worker:$$", $v);
}

sub done_working {
  my ($self) = @_;
  $self->redis->del("worker:$$");
}

# Call when job failed.
sub fail_job {
  my ($self) = @_;
  my $job ||= $self->{job};
  my $v = JSON::encode_json({queue=>$self->{queue}, run_at=>BmUtil::currentTimestamp(),
    payload=>$job, exception=>BmUtil::stackTrace(), worker=>$self->{worker},
    failed_at=>BmUtil::currentTimestamp()});
  $self->redis->rpush("resque:failed", $v);
  $self->redis->incby("resque:stat:failed:$self->{worker}", 1);
  $self->redis->incby("resque:stat:failed", 1);
}

sub retry_queue {
  my ($self, $queue) = @_;
  while (my $obj = $self->dequeue_job("resque:queue:$self->{queue}")) {
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
  my $logrec = join("\t", time(), $self->to_s(), @msg);
  my $logfile = $self->{logfile} || $self->to_s().'.log';
  open(my $lh, ">>", $logfile);
  return print $lh "$logrec\n" unless $lh;
  print $lh "$logrec\n";
  close $lh;
}


1; # End of Package
