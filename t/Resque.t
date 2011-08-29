# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Resque.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;
use lib './lib';

package ResqueTestJob;
sub new {
  my $class = shift;
  my $self = {@_};
  bless $self, $class;
}
sub process { 
  print "Args: @_\n";
  return $_[0]||1;
}
1;

package main;

use Test;
BEGIN { plan tests => 1 };
use Resque;
use Resque::Worker;;
use Resque::Job;
use Data::Dumper;

#ok(1); # If we made it this far, we're ok.
#########################

# Insert your test code below, the Test::More module is use()ed here so read
# its man page ( perldoc Test::More ) for help writing this test script.

my ($r,$j,$p, @j, @w, $w,$m, $n, @a);

# Test Resque
###$r = new Resque(); $r->drop_all(); # Warning: destroys resque namespace
ok $r = new Resque(namespace=>'test');
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
#$j = Resque::Job::create($r, new ResqueTestJob(), 1, "asdf", {a=>1}, {resque=>$r});
$j = $r->create_job(new ResqueTestJob(), 1, "asdf", {a=>1}, 0);
#print Dumper($j);
ok $j->queue('test'), 'test:queue:test';
@a  = $j->args;
ok $a[1], 'asdf';


# Test Resque::Worker ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
$w = new Resque::Worker(namespace=>'test', queues=>'ResqueTestJob', shutdown_on_empty=>0);
$n = $w->to_s;
ok $n, qr/^[\w\.]+:\d+:test$/;
#print 'w=', join(' ', $r->redis->smembers($r->key('workers'))), "\n";
ok $r->redis->sismember($r->key('workers'),$n), 1;

$w->work;
print "end of worker\n";
$w->destroy;


