
use strict;
use Test::More tests => 3;

use_ok("Util::Task");
use_ok("Util::Task::Multi");
use_ok("Util::Task::Simple");

sub use_ok {
    my ($module_name) = @_;

    my $result = eval "use $module_name; 1;";

    ok($result == 1, "Use $module_name");

}
