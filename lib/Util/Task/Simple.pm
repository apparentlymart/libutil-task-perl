
=head1 NAME

Util::Task::Simple - A completely uncoalescable, stupid implementation of Util::Task that just runs a closure.

=head1 SYNOPSIS

    my $task = Util::Task::Simple->new(\&some_sub);

=head1 DESCRIPTION

This Task implementation is about as stupid as they get, just running a given bit of code with no special magic.
Consequently it can't coalesce, and runs all of its tasks sequentially. It also can't be remoted.

It should only be used for quick prototyping, and any uses of it should be replaced with a more sensible
L<Util::Task> subclass before long.

=cut

package Util::Task::Simple;

use strict;
use warnings;
use base qw(Util::Task);

sub new {
    my ($class, $code) = @_;

    return bless $code, $class;
}

sub execute_multi {
    my ($class, $tasks) = @_;

    my $ret = {};
    foreach my $k (keys %$tasks) {
        my $task = $tasks->{$k};
        $ret->{$k} = $task->();
    }
    return $ret;
}

1;
