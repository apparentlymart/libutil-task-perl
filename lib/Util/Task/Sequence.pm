
=head1 NAME

Util::Task::Sequence - A task for running other tasks sequentially.

=head1 SYNOPSIS

    my $base_task = Util::Task::Something->new();
    my $task = Util::Task::Sequence->new($base_task, sub {
        my $result = shift;
        return Util::Task::SomethingElse->new($result);
    });

=head1 DESCRIPTION

In some cases, a task is actually a sequence of smaller task where each
step depends on the previous step. For example, if you want to load
a list of items for a user by username, you might first need to translate
the username into a userid.

While it is in theory possible to build a single task that does both
of these steps, tasks that perform multiple steps are difficult to
batch and coalesce.

This class, which is one of the fundamental building blocks of the
L<Util::Task> framework and is handled as a special case, allows
single tasks to be strung together into a sequence where the result
of one feeds into the next.

When sequence tasks are added to a L<Util::Task::Multi>, the execution
of the multi-task is split into phases, each of which has its own batching
step. Where the constituent tasks support coalescing, the system will also
use solutions found in previous phases to avoid repeating work.

=head1 USAGE

The constructor for this class takes two arguments. The first is a L<Util::Task>
instance that will form the first step in this sequence. The second is
a CODE ref that will recieve the result returned by the first step and should
return either another L<Util::Task> instance for the second step or C<undef>
to indicate that the next step does not need to run and that C<undef> should
be returned for this overall task.

The progression function may itself return a L<Util::Task::Sequence> to
allow multi-step sequences to be created dynamically.

The return value of a sequence task is the return value of the task
returned by the progression function, or C<undef> if the progression
function returns C<undef>.

=cut

package Util::Task::Sequence;

use strict;
use warnings;
use base qw(Util::Task);

sub new {
    my ($class, $base_task, $progress_function) = @_;

    my $self = bless {}, $class;
    $self->{base_task} = $base_task;
    $self->{progress_function} = $progress_function;
    return $self;
}

sub execute {
    my ($self) = @_;

    # This simple execute implementation is provided to satisfy the Task
    # interface, but this class is also handled by the execute() implementation
    # in Util::Task::Multi.

    my $result1 = $self->{base_task}->execute();
    my $next_task = $self->{progress_function}->($result1);
    return $next_task->execute();
}

sub base_task {
    return $_[0]->{base_task};
}

sub progression_function {
    return $_[0]->{progress_function};
}

# If this ever gets called directly then someone's doing something wrong.
# This class is handled as a special case by Util::Task::Multi.
sub execute_multi {
    Carp::croak("Shouldn't call Util::Task::Sequence->execute_multi directly");
}

1;
