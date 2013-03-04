use strict;
use Test::More;
use Test::riak;

undef $ENV{'PATH'};

ok(!defined $Test::riak::errstr, 'no error');
ok(!defined Test::riak->new, 'cannot create instance');
ok($Test::riak::errstr, 'has errstr');

done_testing;
