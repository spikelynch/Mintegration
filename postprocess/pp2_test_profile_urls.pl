#!/usr/bin/perl

=head1 NAME

test_urls.pl

=head1 SYNOPSIS

  [... java app retrieves the raw CSV from staff module... ]

  ./pp1_fix_ids_and_groups.pl -c mintIntConfig.xml
  ./pp2_test_profile_urls.pl  -c mintIntConfig.xml

=head1 DESCRIPTION

After clean_mint_parties has tidied up the raw CSV files, this
script builds staff profile URLs, tests the URLs by scraping them,
removing any links which don't work and writes out the final
people CSV file for harvest by Mint.

It also strips HTML out of the staff biographies in a completely crude
fashion: this was done in a hurry so that we can get it through the
NLA test system.

Tests all of the staff profile URLs in the 'cooked' People file
(configured in mintIntConfig.xml), removes the links which don't work,
and writes out a final version of the file.

=head1 CONFIGURATION

=head2 Environment variables

If any of these is missing, the script won't run:

=over 4

=item MINT_PERLLIB - location of MintUtils.pm

=item MINT_CONFIG  - location of the Mint/RDC config XML file

=item MINT_LOG4J   - location of the log4j.properties file

=back

=head2 Command-line switches

=over 4

=item -c CONFIGFILE - config file (overrides MINT_CONFIG above)

=item -n            - Don't do live URL tests

=item -m MAX        - set a maximum number of records to process.
                      If live testing, this will only count 
                      records with a working URL. 

=item -h            - Print help

=back

=cut


if( ! $ENV{MINT_PERLLIB} || ! $ENV{MINT_CONFIG} || ! $ENV{MINT_LOG4J}) {
	die("One or more missing environment variables.\nRun perldoc $0 for more info.\n");
}

use strict;

use lib $ENV{MINT_PERLLIB};

use Data::Dumper;
use URI;
use Getopt::Std;
use Web::Scraper;
use Log::Log4perl;

use MintUtils qw(read_csv read_mint_cfg write_csv);

my $MAX_TEST = undef;

my $DEFAULT_CONFIG = 'config.xml';

my $LOGGER = 'mintInt.test_urls';

my %opts = ();

getopts("c:m:nh", \%opts) || usage();

if( $opts{h} ) {
    usage();
}

Log::Log4perl::init($ENV{MINT_LOG4J});

my $log = Log::Log4perl->get_logger($LOGGER);

my $config = $opts{c} || $ENV{MINT_CONFIG} || $DEFAULT_CONFIG;

$log->debug("Logging to $LOGGER");

my $really_test = 1;

if( $opts{n} ) {
	$really_test = 0;
}

if( $opts{m} ) {
	if( $opts{m} =~ /^\d+$/ ) {
		$MAX_TEST = $opts{m};
		$log->debug("Maximum tests set to $MAX_TEST");
	} else {
		$log->error("Value passed to -m must be an integer.");
		die("Can't continue.\n");
	}
}


my $mint_cfg   = read_mint_cfg(file => $config) || do {
	$log->fatal("Configuration error.");
	exit(1);
};

$log->trace(Dumper({ config => $mint_cfg }));

my $working_dir = $mint_cfg->{locations}{working} || './';
my $harvest_dir = $mint_cfg->{locations}{harvest} || './';


$working_dir .= '/' unless $working_dir =~ m#/$#;
$harvest_dir .= '/' unless $harvest_dir =~ m#/$#;

$log->debug("Working dir $working_dir");
$log->debug("Harvest dir $harvest_dir"); 


$log->info("Reading people");

my $people = read_csv(
    config => $mint_cfg,
    dir => $working_dir,
    query => 'People',
    file => 'encrypted'
);


$log->info("Reading AOUs");

my $aous = read_csv(
    config => $mint_cfg,
    dir => $harvest_dir,
    query => 'Groups',
    file => 'harvest'
);

$log->info("Testing staff profile URLs");

my $tested = {};

my $n = 0;

PERSON: for my $id ( keys %$people ) {
    if( $people->{$id}{Staff_Profile_Homepage} ) {
    	if( $really_test ) {
			if( !test_url(person => $people->{$id}) ) {
		    	$people->{$id}{Staff_Profile_Homepage} = undef;
			} else {
				$n++;
			}
    	} else {
    		$n++;
    	}
    }
    $tested->{$id} = $people->{$id};
    if( defined $MAX_TEST && $n > $MAX_TEST ) {
    	last PERSON;
    }
}

$log->info("Stripping HTML from descriptions");

PERSON: for my $id ( keys %$people ) {
    my $desc = $people->{$id}{Description};
    my $odesc = $desc;
    my $changes = ( $desc =~ s/<[^>]+>/ /g );
    if( $changes ) {
        if( $desc =~ /</ ) {
            $log->error("[$id] Unsuccessful HTML strip");
        } else {
            $log->warn("[$id] $changes HTML tags removed from bio");
} 
        $people->{$id}{Description} = $desc;
    }
}




$log->info("Writing people harvest file");

write_csv(
    config => $mint_cfg,
    dir => $harvest_dir,
    query => 'People',
    file => 'harvest',
    records => $tested
);

=head1 SUBROUTINES

=over 4

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
	my $result;
	
	eval {
		$result = $get_name->scrape($uri);
	};
	
	if( $@ ) {
		$log->error("[$person->{StaffID} $name] web request failed $url: $@");
		return 0;
	}

    for my $h1 ( @{$result->{head}} ) {
		if( $h1 =~ /$name/ ) {
	    	return 1;
		}
    }
    $log->warn("[$person->{StaffID} $name] profile page not found $url");

    return 0;
}


=item usage()

Command-line usage instructions

=cut

sub usage {
    print <<EOTXT;
$0 [-c config.xml -n -h]

A script which tests each staff profile URL in the CSV of researchers to
be loaded into Mint.  If the page doesn't exist or the researcher's name 
can't be found in a <h1> tag, the URL is removed from the CSV file.

Command-line options:
 
-c FILE  XML config file, default is "$DEFAULT_CONFIG"
-n       Dry run for testing: doesn't actually check the URLs
-h       Print this message

Environment variables:

MINT_PERLLIB 	 - location of the MintUtils.pm library
MINT_CONFIG      - location of the Mint/RDC config XML file
MINT_LOG4J       - location of the log4j.properties file

All of the Mint integration code uses the Log4j logging 
framework (or its Perl emulator) so that logging can be controlled
in a single config gile.  This script's logging id is
'$LOGGER'.


EOTXT

exit(0);


}

=back

=cut
