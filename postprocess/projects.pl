#!/usr/bin/perl

use Text::CSV;

# Format required for Activities harvest:

# ID            - the grant id from funding body, not the RM id
# Submit Year
# Start Year
# Title
# Description
# Institution   - UTS
# Investigators - Probably OK to use just one for now; should be list of all
#                    of them in "Dr AB Chandler; Prof ED Fitzwilliam" format

# Column names in the CSV from researchmaster

my $CCODE         = 0;
my $SPROTITLE     = 1;
my $MDESC         = 2;
my $MKEYWORD      = 3;
my $SFOLIO        = 4;
my $SGRANTTYPE    = 5;
my $SINTCONT_CODE = 6;
my $EXPR1007      = 7;
my $DSTART        = 14;
my $DEND          = 15;

my $BASEURL = '/home/mike/workspace/RDC Mint/test';

my $IN = "$BASEURL/harvest/Mint_Projects.csv";

my $OUT = "$BASEURL/harvest/Activities.csv";

my @OUTHEADS = (
    'ID', 'Submit Year', 'Start Year', 'Title', 'Description',
    'Institution', 'Investigators', 'Discipline' );

my $raw = read_csv($IN) || die;

my $projects = {};

for my $row ( @$raw ) {
    next if $row->[0] !~ /^\d+$/ || $projects->{$row->[$SFOLIO]};

    my $cook_row = [
        $row->[$SFOLIO],
        substr($row->[$CCODE], 0, 4),
        substr($row->[$DSTART], 0, 4),
        $row->[$SPROTITLE],
        $row->[$MDESC],
        'University of Technology, Sydney',
        person($row->[$PERSON]),
        ''
        ];
    $projects->{$row->[$ID]} = $cook_row;

    push @$cooked, $cook_row;
}



my $csv = Text::CSV->new();

open(my $fh, ">:encoding(utf8)", $OUT) || die("Couldn't open $OUT for writing: $!");

$csv->print($fh, \@OUTHEADS);
print $fh "\n";

for my $row ( @cooked ) {
    $csv->print($fh, $row);
    print $fh, "\n";
}

close $fh;





sub person {
    my ( $id ) = @_;

    return $id;
}






sub read_csv {
    my ( $file ) = @_;
    
    my $csv = Text::CSV->new();
    open my $fh, "<:encoding(utf8)", $file || die("$file $!");

    my $rows = [];

    while ( my $row = $csv->getline($fh) ) {
        print join(', ', @$row) . "\n";
        push @$rows, $row;
    }

    close $fh;
    
    return $rows;
}
