#!/usr/bin/perl

=head NAME

people_projects.pl

=head DESCRIPTION

Quick script used to narrow down the list of researcher to those who
have had an ARC grant within a specified year range, so that we can meet
the NLA feed requirement.

=cut

use strict;

use lib '/home/mike/workspace/RDC Data Capture/src/lib/';

use CoataGlue::Person;

use Log::Log4perl qw(:easy);

use Text::CSV;
use Data::Dumper;

Log::Log4perl->easy_init($ERROR);

my $BASEDIR = '/home/mike/workspace/RDC Mint/test';


my $PEOPLE = "$BASEDIR/working/People.raw.csv";

my $PROJECTS = "$BASEDIR/harvest/Mint_Projects.csv";
#my $PROJECTS = '../working/Projects.raw.csv';

my $OUTPUT_PEOPLE = "$BASEDIR/harvest/People_with_projects.csv";
my $OUTPUT_PROJECTS = "$BASEDIR/harvest/Projects.csv";

my $FOLIOCOL = 4;
my $TYPECOL  = 5;
my $SIDCOL   = 6;
my $TITLECOL = 1;
my $STARTCOL = 14;
my $ENDCOL   = 15;

my $FILTERTYPE = '0000015117';
my $FILTERID = '^(LP|DP)';

# my $PROJECT_FILTER = {
#     $FOLIOCOL => $FILTERID
# };

my $PROJECT_FILTER = {
    $TYPECOL => $FILTERTYPE
};

my $CRYPTKEY = '1E23F1709478F63D2083';

my @PEOPLE_HEADERS = qw(

    StaffID SMID IsActive GivenName OtherNames
    FamilyName PrefName Honorific Email JobTitle GroupID_1 GroupID_2
    GroupID_3 ANZSRC_FOR_1 ANZSRC_FOR_2 ANZSRC_FOR_3 URI
    NLA_Party_Identifier ResearcherID openID Personal_URI
    Personal_Homepage Staff_Profile_Homepage Description Projects

);


my $people = {};

for my $record ( read_csv($PEOPLE) ) {
    if( $record->[0] =~ /^\d+$/ ) {
        $people->{$record->[0]} = $record;
    }
}



my $has_projects = {};

PROJECT: for my $record ( read_csv($PROJECTS) ) {
    if( $record->[0] =~ /^\d+$/  ) {
        my ( $ccode, $sid ) = ( $record->[0], $record->[$SIDCOL] );
        my $start = $record->[$STARTCOL];
        if( $start =~ /^(\d\d\d\d)/ ) {
            my $y = $1;
            if( $y lt 2009 ) {
                print "$ccode,$sid,skip,start date = $start\n";
                next PROJECT;
            }
        } else {
            print "$ccode,$sid,skip,Couldn't read start date\n";
            next PROJECT;
        }

        for my $field ( keys %$PROJECT_FILTER ) {
            if( $record->[$field] !~ /$PROJECT_FILTER->{$field}/ ) {
                print "$ccode,$sid,skip,$field $record->[$field] !~ /$PROJECT_FILTER->{$field}/\n";
                next PROJECT;
            }
        }
        if( $people->{$sid} ) {
            push @{$people->{$sid}}, $ccode, $record->[$TYPECOL], $record->[$TITLECOL];
            print "$ccode,$sid,include\n";
            $has_projects->{$sid} = 1;
            
        } else {
            print "$ccode,$sid,skip,CI $sid not found\n";
        }
    }
}

my $csv = Text::CSV->new();

open(my $fh, ">:encoding(utf8)", $OUTPUT_PEOPLE) || die("Couldn't open $OUTPUT_PEOPLE for writing: $!");

$csv->print($fh, \@PEOPLE_HEADERS);
print $fh "\n";


for my $sid ( keys %$people ) {
    if( $has_projects->{$sid} ) {
        my $row = $people->{$sid};
        my $cp = CoataGlue::Person->new(id => $sid);
        my $eid = $cp->encrypt_id(key => $CRYPTKEY);
        $row->[0] = $eid;
        $csv->print($fh, $row);
        print $fh "\n";
    }
}

close $fh;


sub read_csv {
    my ( $file ) = @_;
    
    my $csv = Text::CSV->new();
    open my $fh, "<:encoding(utf8)", $file || die("$file $!");

    my $rows = [];

    while ( my $row = $csv->getline($fh) ) {
        push @$rows, $row;
    }

    close $fh;
    
    return @$rows;
}
