#!/usr/bin/perl

=HEAD NAME

test_urls.pl

=HEAD SYNOPSIS

test_urls.pl

=HEAD DESCRIPTION

Tests all of the staff profile URLs in the 'cooked' People file
(configured in mintIntConfig.xml), removes the links which don't work,
and writes out a final version of the file.

=head SUBROUTINES

=over 4

=cut

use strict;

use lib './extras';
use lib '/home/mike/workspace/FTIUP/lib';

use Data::Dumper;
use URI;
use Getopt::Std;
use Web::Scraper;

use FTIUP::Log;

use MintUtils qw(read_csv read_mint_cfg write_csv);

my $MAX_TEST = undef;

my $DEFAULT_CONFIG = 'config.xml';

my %opts = ();

getopts("c:h", \%opts) || usage();

if( $opts{h} ) {
    usage();
}

my $config = $opts{c} || $DEFAULT_CONFIG;

my $mint_cfg   = read_mint_cfg(file => $config);

my $working_dir = $mint_cfg->{dirs}{working} || './';
my $harvest_dir = $mint_cfg->{dirs}{harvest} || './';

my $log = FTIUP::Log->new(dir => $mint_cfg->{dirs}{logs});

$working_dir .= '/' unless $working_dir =~ m#/$#;
$harvest_dir .= '/' unless $harvest_dir =~ m#/$#;

my $people = read_csv(
    config => $mint_cfg,
    dir => $working_dir,
    query => 'People',
    file => 'raw'
);

my $aous = read_csv(
    config => $mint_cfg,
    dir => $harvest_dir,
    query => 'Groups',
    file => 'harvest'
);

my $n = 0;

for my $id ( keys %$people ) {
    if( $people->{$id}{Staff_Profile_Homepage} ) {
	if( !test_url(person => $people->{$id}) ) {
	    $people->{$id}{Staff_Profile_Homepage} = undef;
	}
    }
    $n++;
    if( $MAX_TEST && $n > $MAX_TEST ) {
        last;
    }
}

write_csv(
    config => $mint_cfg,
    dir => $harvest_dir,
    query => 'People',
    file => 'harvest',
    records => $people
);


=item test_url(person => $person)

Scrape the staff profile URL for a person record and try to find their name
in the h1 tag.

=cut



sub test_url {
    my %params = @_;

    my $person = $params{person};

    my $url = $person->{Staff_Profile_Homepage};
    my $name = join(' ', $person->{Given_Name}, $person->{Family_Name});

    my $get_name = scraper {
	process "h1", "head[]" => 'TEXT';
    };

    my $uri = URI->new($url);

    my $result = $get_name->scrape($uri);

    for my $h1 ( @{$result->{head}} ) {
	if( $h1 =~ /$name/ ) {
	    return 1;
	}
    }
    $log->log("[$person->{StaffID} $name] profile page not found $url");

    return 0;
}


=item usage()

CLI instructions

=cut

sub usage {
    print <<EOTXT;
$0 [-c config.xml -h]

A script which tests each staff profile URL in the CSV of researchers to
be loaded into Mint.  If the page doesn't exist or the researcher's name 
can't be found in a <h1> tag, the URL is removed from the CSV file.

Command-line options:
 
-c FILE  XML config file, default is "$DEFAULT_CONFIG"
-h       Print this message

EOTXT
exit(0);


}
