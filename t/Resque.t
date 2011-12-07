# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Resque.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;
use lib './lib';

my ($r,$j,$p, @j, @w, $w,$m, $n, @a);

use Test;
BEGIN { plan tests => 1 };
use Resque;
use Resque::Worker;;
use Resque::Job;
use Data::Dumper;

# Test Resque ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
###$r = new Resque(); $r->drop_all(); # Warning: destroys resque namespace
$r->drop_all();
ok $r->queue('test'), "test:queue:test";
ok $r->push_queue('test', 'message');
@j = $r->peek('test');
ok scalar(@j), 1;
ok $j[0], qr/message/;

$m = $r->pop_queue('test');
#print Dumper($j);
ok $m, 'message';

$r->drop_queue('test');
ok $r->redis->exists($r->queue('test')), 0;

# Test Resque::Job ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
$j = $r->create_job(new ResqueTestJob(), 1, "asdf", {a=>1}, 0);
ok $j->queue('test'), 'test:queue:test';
@a  = $j->args;
ok $a[1], 'asdf';

# Test Resque::Worker ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
$w = new Resque::Worker(namespace=>'test', queues=>'ResqueTestJob', shutdown_on_empty=>1, test=>1);
$n = $w->to_s;
ok $n, qr/^[\w\.]+:\d+:ResqueTestJob$/;
ok $r->redis->sismember($r->key('workers'),$n), 1;
$w->work;
$w->destroy;

$j = $r->create_job(new ResqueTestJob(), 'hello, queue');
$w = $r->new_worker(queues=>'ResqueTestJob', shutdown_on_empty=>1, test=>1,
  callback=> sub { print "Worker callback: @_\n"; });
$w->work;
$w->destroy;
exit;


package ResqueTestJob; # ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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
