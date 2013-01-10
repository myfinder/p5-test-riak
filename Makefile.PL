use inc::Module::Install;
name 'Test-riak';
all_from 'lib/Test/riak.pm';

# requires '';

tests 't/*.t';
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
