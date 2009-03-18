
use ExtUtils::MakeMaker;
WriteMakefile(
    'NAME' => 'Util::Task',
    'VERSION_FROM' => 'lib/Util/Task.pm',
    'PREREQ_PM'		=> {
    },
    ($] >= 5.005 ?
        (
            ABSTRACT_FROM => 'lib/Util/Task.pm',
            AUTHOR     => 'Martin Atkins <mart@degeneration.co.uk>'
        ) : ()
    ),
);

