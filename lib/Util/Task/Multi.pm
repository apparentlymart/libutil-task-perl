

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
    my $idx = 1;

    # Recursively fill the above data structures, flattening out nested Multi-tasks.
    $self->_prepare_request($batches, $tasks_by_id, $task_ids_by_key, \$idx);

    return [ values(%$batches) ];
}

sub execute {
    my ($self) = @_;

    my $batches = {};
    my $tasks_by_id = {};
    my $task_ids_by_key = {};
    my $idx = 1;

    # Recursively fill the above data structures, flattening out nested Multi-tasks.
    my $task_ids_by_k = $self->_prepare_request($batches, $tasks_by_id, $task_ids_by_key, \$idx);

    my $results = {};

    # By now we've got everything nicely batched up in $batches, so let's execute the batch jobs.
    foreach my $global_batch_key (keys %$batches) {
        my $batch = $batches->{$global_batch_key};

        my ($class, $batch_key, $tasks) = @{$batch};
        $class->execute_multi($batch_key, $tasks, $results);
    }

    # To avoid copying, we prepare the return value inside the $task_ids_by_k hash, since
    # we don't need it anymore.
    my $ret = $task_ids_by_k;

    # This uses the ids it finds in $ret to find the corresponding results and then overwrites the ids
    # with the actual results. By the time this returns, $ret is full of actual results rather than ids.
    $self->_prepare_response($results, $ret);

    return $ret;
}

sub _prepare_request {
    my ($self, $batches, $tasks_by_id, $task_ids_by_key, $idx_ref) = @_;

    my $task_ids_by_k = {};

    print STDERR "My tasks are ".Data::Dumper::Dumper($self->{tasks});

    foreach my $k (keys(%{$self->{tasks}})) {
        my $task = $self->{tasks}{$k};

        if ($task->isa('Util::Task::Multi')) {
            # If we have nested Multi-tasks, flatten it all out so that we can
            # batch the sub-tasks too.
            $task_ids_by_k->{$k} = $task->_prepare($batches, $tasks_by_id, $task_ids_by_key, $idx_ref);
        }
        else {
            my $task_id = $$idx_ref++;
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
    Carp::croak("Shouldn't call Typecore::Task::Multi->execute_multi directly");
}

1;
