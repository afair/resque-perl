package Resque;

use 5.006000;
use strict;
use warnings;

use Redis;
use JSON;
use Resque::Worker;
#use Resque::Job;

require Exporter;

our @ISA = qw(Exporter);

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

sub new {
  my ($class, %config) = @_;
  my $self = {namespace=>'resque', 
    %config};
  bless $self, $class;
}

# Returns the active Redis connection
sub redis {
  my $self = shift;
  unless ($self->{redis}) {
    my $server = $self->{server}||'127.0.0.1:6379';
    $self->{redis} = Redis->new(server=>$server) or die "Can not connect to Redis: $server";
    $self->{redis}->select($self->{database}) if $self->{database};
  }
  $self->{redis};
}

# Convenience method to return a new job on this resque system
sub create_job {
  my ($self, $klass, @args) = @_;
  Resque::Job::create($self, $klass, @args);
}

# Convenience method to return a new worker on this resque system
sub new_worker {
  my ($self, %config) = @_;
  new Resque::Worker(resque=>$self, %config);
}

# Used to decide to inline processing (testing) or queue the request
sub inline {
  my ($self) = @_;
  $self->{config}{inline}||undef;
}

sub key {
  my ($self, @names) = @_;
  my $name = join(':', @names);
  warn "namespace! @names ... ".join(',',caller) if $self->{namespace} eq 'resque'; ### TESTING
  "$self->{namespace}:$name";
}

sub keys {
  my ($self, @names) = @_;
  $self->redis->keys($self->key(@names,"*"));
}

# Returns the key for the queue name (base:queue:name:...)
sub queue {
  my ($self, @name) = @_;
  @name = @{$name[0]} if ref($name[0]) eq 'ARRAY';
  $self->key("queue", @name);
}

# Adds a job item to the named queue
sub push_queue {
  my ($self, $queue, $message) = @_;
  $self->watch_queue($queue);
  $self->redis->rpush($self->queue($queue), $message);
}

# Removes a job item to the named queue and returns it
sub pop_queue {
  my ($self, $queue) = @_;
  $self->redis->lpop($self->queue($queue));
}

sub available_logs {
  my ($self) = @_;
  $self->keys('logs');
}

sub push_log_rec {
  my ($self, $file, $rec) = @_;
  $self->redis->rpush($self->key('log', $file), $rec);
}

sub pop_log_rec {
  my ($self, $log_key) = @_;
  $self->redis->lpop($log_key);
}

# Returns the jobs from the queue
sub peek {
  my ($self, $queue, $start, $count) = @_;
  $count ||= 1;
  my @jobs=();
  if ($count == 1) {
    push @jobs,$self->redis->lindex($self->queue($queue), $start||0);
  }
  else {
    @jobs = $self->redis->lrange($self->queue($queue), $start||0, $count||1);
  }
  #@jobs = map { decode_json($_) } @jobs;
  @jobs;
}

sub queue_size {
  my ($self, $queue) = @_;
  $self->redis->llen($self->queue($queue));
}

sub queues {
  my ($self) = @_;
  $self->redis->smembers($self->key("queues"));
}

sub remove_queue {
  my ($self, $name) = @_;
  $self->redis->srem($self->key("queues"), $self->queue($name));
  delete $self->{watched_queues}{$name};
  $self->redis->del($self->queue($name));
}

# Private: Keep track of queues we've created
sub watch_queue {
  my ($self, $name) = @_;
  $self->{watched_queues} ||= {};
  $self->{watched_queues}{$name} = 1;
  $self->redis->sadd($self->key("queues"), $self->queue($name));
}

# Drops the queue and all jobs in it.
sub drop_queue {
  my ($self, $queue) = @_;
  my $name = $self->queue($queue);
  $self->{watched_queues} ||= {};
  delete $self->{watched_queues}{$name};
  $self->redis->srem($self->key("queues"), $name);
  print "drop name=$name\n";
  $self->redis->del($name);
}

# Drops all subordinate keys for the given key names
sub drop_keys {
  my ($self, @names) = @_;
  my @k = $self->keys();
  #$self->redis->del(@k); # Doesn't take multiple keys like the redis does
  foreach (@k) {
    #print "deleting key $_\n";
    $self->redis->del($_);
  }
}

# Seriously, will drop the entire queue data
sub drop_all {
  my ($self) = @_;
  $self->drop_keys();
}

# Enqueues a job in Resque-standard format
sub enqueue {
  my ($self, $obj, @args) = @_;
  my $queue = $obj->{queue_name} || $obj->queue_name() || ref($obj);
  $self->push_job($queue, {class=>ref($obj), args=>\@args});
}

# Destroys all jobs in the queue matching the arguments
sub dequeue {
  my ($self, $queue, @args) = @_;
  # This doesn't do as advertised
  my $job = $self->pop_job($queue);
  $job ? decode_json($job) : undef;
}

sub reserve {
  my ($self, $queue) = @_;
  $self->{reserve} = $self->dequeue($queue);
}

sub now {
  my ($sec,$min,$hour,$mday,$mon,$year,$wday, $yday,$isdst)=localtime(time);
  sprintf "%4d-%02d-%02d %02d:%02d:%02d", $year+1900,$mon+1,$mday,$hour,$min,$sec;
}

# Appends message to log file
sub logger {
  my ($self, $process, @msg) = @_;
  my $logrec = join("\t", now(), $process, @msg);
  return print "$logrec\n" if $self->{test};

  my $logfile = $self->{logfile} || $ENV{RESQUE_LOG} || $process;
  open(my $lh, ">>", $logfile);
  return print $lh "$logrec\n" unless $lh;
  print $lh "$logrec\n";
  close $lh;
}

###############################################################################
# A "Signal Queue" has one entry per name (like a queue).
# It will process a waiting batch when you don't need multiple jobs triggered.
# It is implemented as a Redis Set with the queue name of "signals".
###############################################################################

# Signal the name to be run at the next cycle
sub set_signal {
  my ($self, @name) = @_;
  $self->redis->sadd($self->queue('signals'), $self->queue(@name));
}

# Returns an array of signals to process
sub signals {
  my ($self) = @_;
  $self->redis->smembers($self->queue('signals'));
}

# Call to unset the signal after processing the resource
sub unset_signal {
  my ($self, @name) = @_;
  $self->redis->srem($self->queue('signals'), $self->queue(@name));
}

###############################################################################
# Cron: Stores a queue message inserted at a given time
# Works with a ISO timestamp: YYYY-MM-DD hh:mm:ss.... or epoch
###############################################################################

# Allows to simple time-based scheduling. It prefers an ISO timestamp, but an 
# epoch will work. At the give, time  (or as soon afterwards as check_cron is 
# called), will insert the msg into the named queue.
sub at_time_do {
  my ($self, $timestamp, $queue, $msg) = @_;
  my $score = substr($timestamp,0,19);
  $timestamp  =~ s/\D//;
  $self->redis->zadd('cron', $score, to_json([$timestamp, $queue, $msg]));
}

# Call check_cron every so often. If a job is ready for running, it will be
# removed from the cron sorted set and pushed into the given queue. The now
# timestamp defaults to the epoch time, so provide it if you are using ISO, etc.
sub check_cron {
  my ($self, $now) = @_;
  $now = substr($now||time(),0,19);
  $now  =~ s/\D//;
  while (my ($member, $score) = $self->redis->zrange('cron', 0, 0, 'withscores')) {
    return if $score > $now;
    my ($timestamp, $queue, $msg) = @{from_json($member)};
    $self->redis->zrem('cron', $member);
    $self->push_queue($queue, $msg);
  }
}


# Preloaded methods go here.

1;
__END__

=head1 Resque

Resque - Perl port of the Resque Queue by Github. Ported from Ruby
for use as a standalone Queuing solution or for integration with Ruby or
other Resque systems.

=head1 SYNOPSIS

  use Resque;

  # Create a resque object for working. The namespace defaults to "resque"
  $r = new Resque(namespace=>'myqueue');

  # Raw Queue operations
  $r->push_queue('test', 'message'); # Writes raw job/message to queue
  @j = $r->peek('test'); # Returns the jobs in the queue
  $m = $r->pop_queue('test'); # Returns next available job/message
  $r->drop_queue('test'); # Destroys all data in the queue

  # Resque Jobs are objects and parameters
  $j = $r->create_job(new ResqueTestJob(), 1, "asdf", {a=>1}, 0);

  # Resque Workers dequeue jobs and process them
  $w = new Resque::Worker(namespace=>'myqueue', queues=>'ResqueTestJob', 
    shutdown_on_empty=>1, test=>1);
  $w->work;
  $w->destroy;

  # The worker invokes Module::perform(@arguments) for each job in the queue
  package ResqueTestJob; 
  my $callback = sub {};
  sub new {
    my $class = shift; my $self = {};
    bless $self, $class;
  }
  sub perform {  # Class method!
    my (@args) = @_;
    print "Args: @args\n";
  }
  1;

  # Alternate worker implementation, takes a closure to run each job
  $j = $r->create_job(new ResqueTestJob(), 'hello, queue');
  $w = $r->new_worker(queues=>'ResqueTestJob', shutdown_on_empty=>1, test=>1,
    callback=> sub { print "Worker callback: @_\n"; });
  $w->work;
  $w->destroy;

  # Use a Time trigger to run a job later
  $r->at_time_do(time()+3600, 'test', 'message');

=head1 DESCRIPTION

Resque acts like the Ruby version of the same name. A process can enqueue a job
onto a named resque queue. One or more workers can run to dequeue a job from the
queue(s) and execute them. The benefit of the Queuing system is that long-running
jobs can be performed asynchronously, or executed on another computer or application.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Resque at L<https://github.com/defunkt/resque>

Redis at L<http://redis.io/>

=head1 AUTHOR

Allen Fair, L<htts://github.com/afair>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Allen Fair

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.3 or,
at your option, any later version of Perl 5 you may have available.


=cut
