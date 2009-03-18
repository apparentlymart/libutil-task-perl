
=head1 NAME

Util::Task - Abstract class representing a possibly-coalescable task.

=head1 SYNOPSIS

    my $task = Util::Task::SomeSubclass->new();
    $task->execute();

=head1 DESCRIPTION

The goal of this class is to allow work to be described in advance of actually doing the work.
The first implication of this is that expensive work (usually I/O) is explicitly executed
rather than hidden away behind innocent-looking accessor methods.

The second implication is that tasks can, in theory, be executed in batch by coalescing
atomic operations into single multi-request calls.

=cut

package Util::Task;

use strict;
use warnings;
use Scalar::Util;

=head1 METHODS

This is an abstract class. Subclasses should override the following methods as appropriate.

=cut

=pod

=head2 $self->execute()

Actually run the deferred task and return the result. This is just a convenience method
for running a single task; it actually does an C<execute_multi> call behind the scenes.
Subclasses should not override this unless they have a good reason to; override C<execute_multi>
instead.

=cut

sub execute {
    my ($self) = @_;

    my ($class, $batch_key) = $self->batching_keys;
    my $results = {};
    $class->execute_multi($batch_key, {r => $self}, $results);
    return $results->{r};
}

=pod

=head2 $self->batching_keys()

When called in batch via L<Util::Task::Multi>, the system will attempt to coalesce
multiple atomic requests into a single batch request.

To do this it needs two pieces of information: the class that will handle the resulting
batch request, and a batching key that allows that class to batch its tasks
into multiple distinct buckets. Tasks within a given multi-task set that have the
same ($class, $task_key) tuple will ultimately be handled by a single call to
C<$class->execute_multi>.

It is recommended to also include a task key that uniquely identifies the operation
that this specific task instance will perform when combined with the $class and $coalesce_key.
If included, the system will assume that multiple tasks with the same task key can be handled by a single call.
Otherwise, each instance will be handled separately.

Overriden versions of this method should return a list of (C<$class>, C<$coalesce_key>, C<$task_key>),
where C<$task_key> is optional. The default is to return the class which C<$self> belongs to
as the class, 'default' as the batching key (which causes all tasks of this class to be handled
in a single batch) and no task key.

=cut

sub batching_keys {
    my ($self) = @_;
    return (Scalar::Util::blessed($self), 'default', undef);
}

=pod

=head2 $class->execute_multi($batch_key, $tasks, $results)

Given an HASH ref of keys mapped to task instances that belong to classes that returned $class from their
coalesce_class method, execute all of the tasks in the most efficient way possible and insert the results
into C<$results> (a HASH ref) with the keys matching the corresponding tasks in $tasks.
Tasks should be designed to never use exceptions to signal failure.

The $batch_key is the class-specific batching key that was returned by the C<keys> method
on all of the supplied tasks. It's included for convenience though it's also available by explicitly calling
C<keys> on any of the supplied tasks. In the default implementation of C<keys>, this
is the string 'default'.

This should not be called directly. Instead, use L<Util::Task::Multi> to create a single
task that represents the set of tasks you wish to execute and call C<execute()> on it. This will
allow the task set to be optimized and dispatched to the correct task classes.

The request keys passed in should only contain word characters. This is not currently checked, but
other keys may conflict with reserved keys used internally and make weird things happen.

FIXME: Figure out what should happen if execute_multi *does* C<die>.

All subclasses that are returned by some implementation of C<coalesce_class> must override this.
The default implementation just dies.

=cut

sub execute_multi {
    my ($class, $tasks) = @_;
    die "No execute_multi() implementation for $class";
}

1;
