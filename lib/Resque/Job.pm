package Resque::Job;

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

sub to_s {
  my ($self) = @_;
  $to_s ||= join(":", hostname(), $$, join(',',$self->{queues}));
}


1; # End of Package
