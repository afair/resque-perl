# Before `make install' is performed this script should be runnable with
# `make test'. After `make install' it should work as `perl Resque.t'

#########################

# change 'tests => 1' to 'tests => last_test_to_print';

use strict;
use warnings;

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

my ($r,$j,$p, @j, @w, $w);

# Test Resque
ok $r = new Resque(namespace=>'test');
$r->drop_all();
ok $r->queue('test'), "test:queue:test";
ok $r->push_job('test', {class=>'Resque', args=>[]});
@j =  Dumper( $r->peek('test'));
ok scalar(@j), 1;
ok $j[0], qr/Resque/;

$j = $r->pop_job('test', {class=>'Resque', args=>[]});
#print Dumper($j);
ok $j->{class}, 'Resque';

$r->drop_queue('test');
ok $r->redis->exists($r->queue('test')), 0;

# Test Resque::Worker~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
$w = new Resque::Worker(namespace=>'test', queues=>'test');
ok $w->to_s, qr/^[\w\.]+:\d+:test$/;

$w->destroy;


