#!/usr/bin/perl

use strict;

use Text::CSV;
use Data::Dumper;

# collate projects file.  This should someday be gussied up to use the
# same framework as the other Perl scripts

# Format required for Activities harvest:

# ID            - the grant id from funding body, not the RM id
# Submit Year
# Start Year
# Title
# Description
# Institution   - UTS
# Investigators - list of all of them in "Dr AB Chandler; Prof ED Fitzwilliam" format
# Discipline

# Column names in the CSV from researchmaster

use constant {
    CCODE          => 0,
    SPROTITLE      => 1,
    MDESC          => 2,
    MKEYWORD       => 3, 
    SFOLIO         => 4,
    SGRANTTYPE     => 5,
    SINTCONT_CODE  => 6,
    PRIVATE        => 7,
    SSTATUS_NAME   => 8,
    LCURRENT_LOGIC => 9,
    LPRIVATE_LOGIC => 10,
    LCLOSE_LOGIC   => 11,
    DAPPLIC        => 12,
    DSTART         => 13, 
    DEND           => 14,
    CPERSON_NAME   => 15,
    CPERSON_CODE   => 16,
    NORDER         => 17
};


my $CUTOFF_YEAR = 2009;


my $UTS_NAME = 'University of Technology, Sydney';


my $BASEURL = '/home/mike/workspace/RDC Mint/test';

my $IN = "$BASEURL/working/Mint_Project_With_All_CIs.csv";

my $OUT = "$BASEURL/harvest/Activities.csv";

my @OUTHEADS = (
    'ID', 'Submit Year', 'Start Year', 'Title', 'Description',
    'Institution', 'Investigators', 'Discipline' );


my $raw_projects = read_csv($IN);

my $projects = {};
my $people = {};
my $ids = {};

my $tick = 0;

print "Projects\n";

for my $row ( @$raw_projects ) {
    my $id =    $row->[CCODE];

    $tick++;
    my $stick = substr(' ' . $tick, -2);
    if( $stick eq '00' ) {
        print "$tick...\n";
    }

    next unless $id;
    
    if( !$projects->{$id} ) {
        $projects->{$id} = [
            $row->[SFOLIO],
            substr($row->[DAPPLIC], 0, 4),
            substr($row->[DSTART], 0, 4),
            $row->[SPROTITLE],
            $row->[MDESC],
            $UTS_NAME,
            $row->[MKEYWORD]
        ];
        $people->{$id} = [];
        if( my $folio = $row->[SFOLIO] ) {
            $ids->{$folio}{$id} = 1;
        }
    }

    my $name =  $row->[CPERSON_NAME];
    my $sid =   $row->[CPERSON_CODE];
    my $order = $row->[NORDER];

    $people->{$id}[$order] = $row->[CPERSON_NAME];

}


print "Removing projects that share a SFOLIO: \n";

for my $sfolio ( sort keys %$ids ) {
    if ( scalar( keys %{$ids->{$sfolio}} ) > 1 ) {
        warn "$sfolio: " . join(', ', keys %{$ids->{$sfolio}}) . "\n";
        for my $id ( keys %{$ids->{$sfolio}} ) {
            delete $projects->{$id};
        }
    }
}

print "Removing projects with no SFOLIO: \n";

for my $id ( keys %$projects ) {
    if( !$projects->{$id}[0] ) {
        warn("Removed $id\n");
        delete $projects->{$id};
    }
}

my $n = scalar(keys %$projects);

print "Got $n projects\n";


print "Collating investigators...\n";

for my $id ( sort keys %$people ) {
    my $names = join('; ', @{$people->{$id}});
    my $disc = pop @{$projects->{$id}};
    push @{$projects->{$id}}, $names, $disc;
}

print "Writing to $OUT...\n";

my $csv = Text::CSV->new({eol => $/});

my $fh;

open(my $fh, ">:encoding(utf8)", $OUT) || die(
    "Couldn't open $OUT for writing: $!"
);

$csv->print($fh, \@OUTHEADS);


for my $project ( values %$projects ) {
    $csv->print($fh, $project);
}

close $fh;



print "Done.\n";


sub read_csv {
    my ( $file ) = @_;
    
    print "Reading $file\n";
    my $csv = Text::CSV->new();
    my $fh;
    open($fh, "<:encoding(utf-8)", $file) || die("Couldn't open $file $!");

    my $rows = [];
    my $lastrow = undef;

    while ( my $row = $csv->getline($fh) ) {
        if( substr($row->[DSTART], 0, 4) < $CUTOFF_YEAR ) {
            next;
        }
        push @$rows, $row;
    }

    close $fh;

    print "Got " . scalar(@$rows) . " rows.\n";

    return $rows;
}


