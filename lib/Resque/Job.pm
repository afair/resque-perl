package Resque::Job;

use 5.006000;
use strict;
use warnings;

use JSON; #::XS;
#use lib "$ENV{HOME}/src/Resque";
#use Resque;
use Resque::Failure;
use Data::Dumper;

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

my $to_s;

sub new {
  my ($class, $queue, $payload, %config) = @_;
  my $job = $payload ? decode_json($payload) : {};
  my $self = {queue=>$queue, payload=>$payload, job=>$job, %config};
  $self = $config{resque} ? {%{$config{resque}},%$self} : new Resque(%$self);
  bless $self, $class;
}

# Returns the JSON-encoded string of the job message:
#   "{class:'classname', args:['arg1',...]}
sub job_string {
  my ($klass, @args) = @_;
  encode_json({class=>$klass, args=>\@args, queued_at=>time(), tries=>0});
}

# Returns a JSON string serializing this Job instance with data.
sub to_s {
  my ($self) = @_;
  encode_json($self->{job});
}

# Creates a Job my placing it on a queue. Resque::Job::create(Class, *args..., {opt=>value,...})
sub create {
  my ($resque, $klass, @args) = @_;
  my $opt = ref($args[-1]) eq 'HASH' ? pop(@args) : {};
  return perform($klass, @args) if $resque->inline;
  $klass = ref($klass) if ref($klass);
  my $queue = $opt->{queue} || "$klass";
  my $job = new Resque::Job($queue, job_string($klass, @args), resque=>$resque, %$opt);
  $job->enqueue();
  $job;
}

sub enqueue {
  my ($self, $queue) = @_;
  $queue ||= $self->{queue};
  $self->push_queue($queue, $self->to_s);
}

# Removes all matching jobs from the queue, returns number of destroyed jobs
sub destroy {
  my ($resque, $queue, $klass, @args) = @_;
  my $destroyed=0;
  if (@args) {
    $destroyed = $resque->redis->lrem($resque->key('queue', $queue), 0, job_string($klass, @args));
  }
  else {
    my @payloads = $resque->redis->lrange($resque->key('queue', 0, -1));
    foreach my $p (@payloads) {
      my $job = decode_json($p);
      $destroyed += $resque->redis.lrem($resque->key('queue', 0, $p)) if $job->{class} eq $klass;
    }
  }
  $destroyed;
}

# Returns a job instance from the queue, or undef if none available
sub reserve {
  my ($resque, $queue) = @_;
  my $payload = $resque->pop_queue($queue);
  return undef unless $payload;
  new Resque::Job($queue, $payload, resque=>$resque);
}

sub args {
  #print Dumper(@_);
  @{$_[0]->{job}{args}||[]};
}

sub perform {
  my ($self) = @_;
  my $job_was_performed = 0;
  my $result;
  eval {
    no strict 'refs';
    $result = &{"$self->{job}{class}::perform"}($self->args);
    $job_was_performed = 1;
    use strict;
  };
  if ($@) { # Failure
    $self->fail($@);
  }
  $result;
}

sub fail {
  my ($self, $exception) = @_;
  #print "job failed :-( $exception\n";
  Resque::Failure::create(payload=>$self->{payload}, exception=>$exception, 
    worker=>$self->{worker}, queue=>$self->{queue}, retries=>$self->{retries}||0);
}


1; # End of Package
