package Test::riak;
use strict;
use warnings;

use Class::Accessor::Lite;
use Cwd;
use File::Temp qw/tempfile tempdir/;
use Test::TCP;

our $VERSION = '0.03';
our $errstr;
our $app_config_tmpl=<<'EOS';
[
    %% for protocol buffer
    {riak_api, [
        {pb_ip, "127.0.0.1" },
        {pb_port, __PB_PORT__}
    ]},
    %% riak_core
    {riak_core, [
        {ring_state_dir, "__TMP_DIR__/riak"},
        {http, [ {"0.0.0.0", __HTTP_PORT__} ]},
        {platform_data_dir, "__TMP_DIR__/riak"},
        {platform_log_dir, "__TMP_DIR__/riak"}
    ]},
    %% riak_backend_kvs
    {riak_kv, [
        {storage_backend, riak_kv_memory_backend}
    ]},
    %% crash log
    {lager, [
        {handlers, [
            {lager_console_backend, info},
            {lager_file_backend, [
                {"__TMP_DIR__/error.log", error, 10485760, "$D0", 5},
                {"__TMP_DIR__/console.log", info, 10485760, "$D0", 5}
            ]}
        ]},
        {crash_log, "__TMP_DIR__/riak/crash.log"}
    ]}
].
EOS
my %Defaults = (
    auto_start => 1,
    base_dir   => undef,
    bin_dir    => '',
    launch_cmd => undef,
    riak_prog  => undef,
    app_config => undef,
    pb_port    => undef,
    http_port  => undef,
    _owner_pid => undef,
);

$SIG{INT}  = 'stop';
$SIG{TERM} = 'stop';

Class::Accessor::Lite->mk_accessors(keys %Defaults);

sub new {
    my $klass = shift;
    my $self = bless {
        %Defaults,
        @_ == 1 ? %{$_[0]} : @_,
        _owner_pid => $$,
    }, $klass;

    if (defined $self->base_dir) {
        $self->base_dir(cwd . '/' . $self->base_dir) if $self->base_dir !~ m|^/|;
    } else {
        $self->base_dir(
            tempdir(
                CLEANUP => $ENV{TEST_RIAK_PRESERVE} ? undef : 1,
            ),
        );
    }

    my $pb_port = empty_port;
    $self->pb_port($pb_port);
    my $http_port = empty_port;
    $self->http_port($http_port);
    my $app_config = $app_config_tmpl;
    my $dir        = $self->base_dir;
    $app_config    =~ s/__PB_PORT__/$pb_port/g;
    $app_config    =~ s/__HTTP_PORT__/$http_port/g;
    $app_config    =~ s/__TMP_DIR__/$dir/g;
    my ($fh, $filename) = tempfile(DIR => $self->base_dir, SUFFIX => '.config');
    $fh->print($app_config);
    $self->app_config($filename);

    my $riak_prog = _find_program(qw/riak/) or return;
    $self->riak_prog($riak_prog);

    if ($self->auto_start) {
        $self->setup;
        $self->start;
    }
    else {
        $self->setup;
    }

    $self;
}

sub DESTROY {
    my $self = shift;
    $self->stop if $$ == $self->_owner_pid;
}

sub setup {
    my $self = shift;

    my $runner_base_dir;
    my $runner_script_dir;
    open my $fh_riak, '<', $self->riak_prog;
    while (<$fh_riak>) {
        $runner_base_dir = $_ if $_ =~ /^RUNNER_BASE_DIR=/;
        $runner_script_dir = $_ if $_ =~ /^RUNNER_SCRIPT_DIR=/;
    }
    close $fh_riak;

    my $ert_base;
    if ($runner_base_dir =~ /^RUNNER_BASE_DIR=\//) {
        $runner_base_dir =~ s/^RUNNER_BASE_DIR=//g;
        $ert_base = $runner_base_dir;
    }
    else {
        my @conf = split '=', $runner_script_dir;
        my $path = $conf[1];
        $path =~ s/(^.*)\/.*/$1/;
        $ert_base = $path;
    }
    chomp $ert_base;

    my $start_erl_data;
    open my $fh_start_erl, '<', $ert_base.'/releases/start_erl.data';
    while(<$fh_start_erl>) {
        $start_erl_data = $_;
    }
    close $fh_start_erl;
    chomp $start_erl_data;

    my @vsns = split ' ', $start_erl_data;
    my $erts_vsn = $vsns[0];
    my $app_vsn = $vsns[1];

    my $root_dir = $ert_base;
    my $bin_dir  = $root_dir .'/erts-'. $erts_vsn .'/bin';
    $self->bin_dir($bin_dir);

    my $config_path = $self->app_config;
    my $base_path   = $ert_base;
    my $base_dir    = $self->base_dir;

    $ENV{EMU}            = 'beam';
    $ENV{ROOTDIR}        = $ert_base;
    $ENV{BINDIR}         = $bin_dir;
    $ENV{PROGNAME}       = 'riak';
    $ENV{ERL_CRASH_DUMP} = $base_dir;
    
    $self->launch_cmd(
        $bin_dir.'/run_erl -daemon '.$base_dir.' '.$base_dir.' "'
        .$bin_dir.'/erlexec -boot '
        .$base_path.'/releases/'
        .$app_vsn.'/riak -embedded -config '
        .$config_path.' -pa '
        .$base_path.'/lib/basho-patches -name riak@127.0.0.1 +A 64 -- console"');

    1;
}

sub start {
    my $self = shift;

    system $self->launch_cmd;
}

sub stop {
    my ($self) = @_;

    my $to_erl = $self->bin_dir.'/to_erl';
    my $base_dir = $self->base_dir;
    system 'echo "init:stop()." | '."$to_erl $base_dir > /dev/null 2>&1";
}

sub _find_program {
    my ($prog) = shift;
    undef $errstr;

    if ($ENV{PATH}) {
        $ENV{PATH} = $ENV{PATH}.':/usr/sbin:/usr/bin:/usr/local/sbin:/usr/local/bin';
    }

    my $path = `which $prog 2> /dev/null`;
    chomp $path if $path;

    if ($path) {
        return $path;    
    }
    else {
        $errstr = "$prog: not found";
        return;
    }
}

1;
__END__

=head1 NAME

Test::riak - Riak runner for tests

=head1 SYNOPSIS

  use Test::riak;
  use Data::Riak::Fast;
  
  my $runner = Test::riak::new;
  my $bucket = Data::Riak::Bucket->new({
      name => 'test_bucket',
      riak => Data::Riak::Fast->new({
          transport => Data::Riak::HTTP->new({
              host => '127.0.0.1',
              port => $runner->http_port,
          }),
      })
  });

  $bucket->add('foo', 'bar');
  ...


=head1 DESCRIPTION

Test::riak is Riak runner for tests

=head1 AUTHOR

Tatsuro Hisamori E<lt>myfinder@cpan.orgE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
