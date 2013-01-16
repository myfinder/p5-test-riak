use inc::Module::Install;
name 'Test-riak';
all_from 'lib/Test/riak.pm';

requires 'Class::Accessor::Lite';
requires 'Test::TCP';

tests_recursive;
author_tests 'xt';

test_requires 'Test::More';
auto_set_repository;
auto_include;
WriteAll;
