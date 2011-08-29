# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Resque.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;
use lib './lib';

package ResqueTestJob;
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

my ($r,$j,$p, @j, @w, $w,$m, $n);

# Test Resque
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

# Test Resque::Worker~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
$w = new Resque::Worker(namespace=>'test', queues=>'test');
$n = $w->to_s;
ok $n, qr/^[\w\.]+:\d+:test$/;
print 'w=', join(' ', $r->redis->smembers($r->key('workers'))), "\n";
ok $r->redis->sismember($r->key('workers'),$n), 1;

$w->destroy;


