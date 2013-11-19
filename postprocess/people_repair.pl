#!/usr/bin/perl

# quickie to repair the unquoted-comma damage in the NLAids people file


use Text::CSV;
use Data::Dumper;

my $INFILE = '../test/harvest/People_NLAIDs_20131119.csv';

my $OUTFILE = './People_NLAIDs_repaired.csv';

my $records = read_csv($INFILE) || die;

for my $row ( @$records ) {
    if( $row->[0] =~ /^http/ ) {
        my @bits = ();
        for my $bit ( splice(@$row, 21) ) {
            if( $bit ) {
                push @bits, $bit;
            }
        }
        my $description = join(', ', @bits);
        $row->[21] = $description;
    }
}

 
write_csv($OUTFILE, $records);       



sub read_csv {
    my ( $file ) = @_;
    
    my $csv = Text::CSV->new();

    open my $fh, "<:encoding(utf8)", $file or die "$file: $!";

    my $rows = [];

    while ( my $row = $csv->getline( $fh ) ) {
        push @$rows, $row;
    }
    close $fh;
    return $rows;
}


sub write_csv {
    my ( $file, $records ) = @_;

    my $csv = Text::CSV->new();

    $csv->eol ("\r\n");

    open $fh, ">:encoding(utf8)", $file or die "$file: $!";
    $csv->print ($fh, $_) for @$records;

    close $fh or die "$file: $!";

    return 1;
}
