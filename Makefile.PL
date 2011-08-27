use 5.006000;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Resque',
    VERSION_FROM      => 'lib/Resque.pm', # finds $VERSION
    PREREQ_PM         => {'Test'        =>  0, }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Resque.pm', # retrieve abstract from module
       AUTHOR         => 'Allen Fair <allen@nonet>') : ()),
);
