package Resque;

use 5.006000;
use strict;
use warnings;

use Redis;
use Resque::Worker;
use JSON::XS;

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
    my $server = $self->{config}{server}||'127.0.0.1:6379';
    $self->{redis} = Redis->new(server=>$server) or die "Can not connect to Redis: $server";
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

sub queue {
  my ($self, $name) = @_;
  $self->key("queue", $name);
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
  $self->redis->srem($self->key("queues"), $self->queue($name));
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

  my $logfile = $self->{logfile} || $ENV{RESQUE_LOG} || "$process.log";
  open(my $lh, ">>", $logfile);
  return print $lh "$logrec\n" unless $lh;
  print $lh "$logrec\n";
  close $lh;
}

# Preloaded methods go here.

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

Resque - Perl extension for blah blah blah

=head1 SYNOPSIS

  use Resque;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for Resque, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Allen Fair, E<lt>allen@nonetE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Allen Fair

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.12.3 or,
at your option, any later version of Perl 5 you may have available.


=cut
