

=head1 NAME

Util::Task::Multi - A special task that actually runs multiple tasks

=head1 SYNOPSIS

    my $task = Util::Task::Multi->new();
    $task->add_subtask(task1 => Util::Task::Something->new());
    $task->add_subtask(task2 => Util::Task::SomethingElse->new());
    $task->execute();

=head1 DESCRIPTION

This task subclass allows multiple atomic tasks to be run as a single task, coalescing them
in the most efficient way possible.

=cut

package Util::Task::Multi;

use strict;
use warnings;
use base qw(Util::Task);
use Carp;

sub new {
    my ($class) = @_;

    return bless {
        tasks => {},
    }, $class;
}

sub add_subtask {
    my ($self, $k, $task) = @_;

    $self->{tasks}{$k} = $task;
}

# This is provided to allow developers to analyse the batching behavior of a particular multi-task,
# to find out how the task will be executed and analyse how well the batcher is performing.
# (In other words, this is our equivalent of EXPLAIN.)
sub batches_for_debugging {
    my ($self) = @_;

    my $batches = {};
    my $tasks_by_id = {};
    my $task_ids_by_key = {};
    my $progressions_by_id = {};
    my $id_ref_by_id = {};
    my $idx = 1;

    # Recursively fill the above data structures, flattening out nested Multi-tasks.
    $self->_make_batches($self->{tasks}, $batches, $tasks_by_id, $task_ids_by_key, $progressions_by_id, $id_ref_by_id, \$idx);

    return [ values(%$batches) ];
}

sub execute {
    my ($self) = @_;

    my $batches = {};
    my $tasks_by_id = {};
    my $task_ids_by_key = {};
    my $progressions_by_id = {};
    my $id_ref_by_id = {};
    my $idx = 1;

    my $tasks = $self->{tasks};

    # Recursively fill the above data structures, flattening out nested Multi-tasks.
    my $task_ids_by_k = $self->_make_batches($tasks, $batches, $tasks_by_id, $task_ids_by_key, $progressions_by_id, $id_ref_by_id, \$idx);

    my $results = {};

    while (%$batches) {

        # By now we've got everything nicely batched up in $batches, so let's execute the batch jobs.
        foreach my $global_batch_key (keys %$batches) {
            my $batch = $batches->{$global_batch_key};

            my ($class, $batch_key, $tasks) = @{$batch};
            $class->execute_multi($batch_key, $tasks, $results);
        }

        if (%$progressions_by_id) {
            # If there are any progressions, then we need to run another phase.
            my $next_tasks = {};
            foreach my $task_id (keys %$progressions_by_id) {
                my $progression = $progressions_by_id->{$task_id};
                my $intermediate_result = $results->{$task_id};
                $results->{$task_id} = undef;
                my $next_task = $progression->($intermediate_result);

                if (defined($next_task)) {
                    $next_tasks->{$task_id} = $next_task if defined($next_task);
                }
                else {
                    # Leave the result as undef and carry on.
                }
            }

            # Reset and calculate the batches for the next phase.
            $batches = {};
            $tasks_by_id = {};
            $progressions_by_id = {};
            # We intentionally don't reset $id_ref_by_id because we're going to use it
            # to update the input-keys-to-task-ids mapping in a moment.
            # We also leave task_ids_by_key so that we won't re-run coalescable tasks
            # that we've already run.
            my $task_ids_by_original_task_id = $self->_make_batches($next_tasks, $batches, $tasks_by_id, $task_ids_by_key, $progressions_by_id, $id_ref_by_id, \$idx);

            # Update $task_ids_by_k to point at the new task ids rather than the old,
            # so that when we're done we use the final result.
            foreach my $old_task_id (%$task_ids_by_original_task_id) {
                if (my $id_ref = $id_ref_by_id->{$old_task_id}) {
                    $$id_ref = $task_ids_by_original_task_id->{$old_task_id};
                }
            }
        }
        else {
            # We're done!
            last;
        }

    }

    # To avoid copying, we prepare the return value inside the $task_ids_by_k hash, since
    # we don't need it anymore.
    my $ret = $task_ids_by_k;

    # This uses the ids it finds in $ret to find the corresponding results and then overwrites the ids
    # with the actual results. By the time this returns, $ret is full of actual results rather than ids.
    $self->_prepare_response($results, $ret);

    return $ret;
}

sub _make_batches {
    my ($self, $tasks, $batches, $tasks_by_id, $task_ids_by_key, $progressions_by_id, $id_ref_by_id, $idx_ref) = @_;

    my $task_ids_by_k = {};

    foreach my $k (keys(%$tasks)) {
        my $task = $tasks->{$k};

        # If the caller passed in an $idx_ref then they want us to assign ids. Otherwise, the ids
        # are already assigned in $k.
        my $task_id = $$idx_ref++;

        if ($task->isa('Util::Task::Sequence')) {
            # If we have a sequence, then we make a note that it's a sequence and then
            # treat it as if it were its base step for the purposes of batching.
            $progressions_by_id->{$task_id} = $task->progression_function;
            $task = $task->base_task;
        }

        if ($task->isa('Util::Task::Multi')) {
            # If we have nested Multi-tasks, flatten it all out so that we can
            # batch the sub-tasks too.
            $task_ids_by_k->{$k} = $task->_make_batches($task->{tasks}, $batches, $tasks_by_id, $task_ids_by_key, $progressions_by_id, $id_ref_by_id, $idx_ref);
        }
        else {
            my ($class, $batch_key, $task_key) = $task->batching_keys;
            my $global_batch_key = join("\t", $class, $batch_key);
            my $global_task_key = defined($task_key) ? join("\t", $global_batch_key, $task_key) : undef;
            $batches->{$global_batch_key} ||= [$class, $batch_key, {}];

            # Unless we've already encountered another instance of this task
            unless ($global_task_key && $task_ids_by_key->{$global_task_key}) {
                $task_ids_by_key->{$global_task_key} = $task_id if $global_task_key;
                $tasks_by_id->{$task_id} = $task;
                $batches->{$global_batch_key}[2]{$task_id} = $task;
            }

            $task_ids_by_k->{$k} = $task_id;
            $id_ref_by_id->{$task_id} = \$task_ids_by_k->{$k};
        }
    }

    return $task_ids_by_k;
}

sub _prepare_response {
    my ($self, $results, $response) = @_;

    foreach my $k (keys %$response) {
        my $id = $response->{$k};

        if (ref $id eq 'HASH') {
            # This was a nested multi-task, so let's reconstruct the tree.
            my $sub_response = $id;
            $self->_prepare_response($results, $sub_response);
        }
        else {
            $response->{$k} = $results->{$id};
        }
    }
}

# If this ever gets called directly then someone's doing something wrong.
# execute_multi is only intended to be used by this class's execute implementation.
sub execute_multi {
    Carp::croak("Shouldn't call Util::Task::Multi->execute_multi directly");
}

1;
