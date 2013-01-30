#!/usr/bin/perl

=HEAD NAME

clean_mint_parties.pl

=HEAD DESCRIPTION

A script to perform the following data-cleaning operations on the
feeds for parties (people and groups) after they are fetched from the
Staff Module and before they are harvested by Mint:

=over 4

=item Throw away old AOUs marked "DO NOT USE"

=item Generate staff profile URLs

=back

=head SUBROUTINES

=over 4

=cut

use strict;

use lib './extras';
use lib '/home/mike/workspace/FTIUP/lib';

use Data::Dumper;
use Getopt::Std;
use FTIUP::Log;

use MintUtils qw(read_csv read_mint_cfg write_csv);

my $DEFAULT_CONFIG = 'config.xml';

my %opts = ();

getopts("c:dh", \%opts) || usage();

if( $opts{h} ) {
    usage();
}

my $config = $opts{c} || $DEFAULT_CONFIG;

my $mint_cfg   = read_mint_cfg(file => $config);

if( $opts{d} ) {
    print "Dumping config:\n";
    print Dumper($mint_cfg);
    exit(0);
}


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
    dir => $working_dir,
    query => 'Groups',
    file => 'raw'
);

my $mus = read_csv(
    config => $mint_cfg,
    dir => $working_dir,
    query => 'ManagingUnits',
    file => 'raw'
);


for my $muid ( keys %$mus ) {
    if( $aous->{$muid} ) {
	$log->log("MU/AOU key clash: $muid\n");
    } else {
	$aous->{$muid} = $mus->{$muid}
    }
}


clean_aous(
    aous => $aous,
    config => $mint_cfg->{groupTidy}
);


make_urls(
    aous => $aous,
    people => $people,
    config => $mint_cfg->{landingPageURLs}
);

write_csv(
    config => $mint_cfg,
    dir => $harvest_dir,
    query => 'Groups',
    file => 'harvest',
    records => $aous
);

write_csv(
    config => $mint_cfg,
    dir => $working_dir,
    query => 'People',
    file => 'raw',
    records => $people
    );

$log->log("Done.\n");


=item usage()

CLI instructions

=cut

sub usage {
    print <<EOTXT;
$0 [-c config.xml -d -h]

A script to perform the following data-cleaning operations on the
feeds for parties (people and groups) after they are fetched from the
Staff Module and before they are harvested by Mint:

* Remove faculty prefixes from AOU name
* Throw away old AOUs marked "DONT USE"
* Generate staff profile URLs

Command-line options:
 
-c FILE  XML config file, default is "$DEFAULT_CONFIG"
-d       Dump the config data structure and quit
-h       Print this message

EOTXT
exit(0);


}

=item clean_aous(aous => $aous)

Clean the aou records: make sure faculty prefixes are consistent, drop
any aous marked "DO NOT USE" and any other tidying.

Also reads the MU names from the Parent_Group_Name field and adds them
in as records (because Mint/RIF-CS don't have different levels of group
record like ResearchMaster)

=cut


sub clean_aous {
    my %params = @_;

    my $aous = $params{aous};
    my $config = $params{config};

    die("clean_aous needs a config hash") unless $config;

    my $ignore_re = qr/$config->{ignore}/;

  AOU: for my $aouID ( keys %$aous ) {
	my $aou = $aous->{$aouID};
	my $name = $aou->{Name};

	if( $name =~ /$ignore_re/ ) {
	    $log->log("Ignoring AOU $aouID $name\n");
	    delete $aous->{$aouID};
	    next AOU;
	}

	if( $name =~ /\|/ ) {
	    ( $name ) = split(/\|/, $name);
	    $log->log("Split AOU with |: '$name'");
	}

	my $parent = $aou->{Parent_Group_ID};

	if( $parent && !$aous->{$parent} ) {
	    $log->log("AOU $aouID - $name parent group ID $parent not found.\n");
	} else {
	    if( $name =~ /^RS/ || $name =~ /Associate|Member|Core/ ) {
		# For now, delete Research Strength AOUs, because the
		# query that generates the raw People file is not linking 
		# to them.

		$log->log("Removing RS '$name'");
		delete $aous->{$aouID};
		next AOU;
	    }

	    # Make sure that all divisions with a Faculty parent
	    # get a standardised Faculty prefix

	    if( $name =~ /^([A-Z ]+)\.(.*)$/ ) {
		if( my $pref = $config->{prefixes}{$1} ) {
		    $name = join('.', $pref, $2);
		    $log->log("Changed prefix $1 to $pref in '$name'");
		}
	    } else {
		if( $parent && $config->{prefixes}{$parent} ) {
		    $log->log("Prefixed $config->{prefixes}{$parent} to '$name'");
		    $name = join('.', $config->{prefixes}{$parent}, $name);
		} else {
		    $log->log("Warning '$name' without prefix");
		}
	    }
	}
	$aou->{Name} = $name;
    }
}






=item make_urls(people => $people, aous => $aous, config => $config)

Tries to make staff profile URLs for each record in people, by matching
them against an AOU, looking for that AOU's parent faculty, and then filling
out a URL from the config file (each faculty has a separate staff
directory, although this is expected to change in 2013).

If a URL can't be made (because the staff member doesn't belong to an AOU
which can be connected to a faculty) they stay in the data but a warning
is sent to STDERR.

=cut


sub make_urls {
    my %params = @_;

    my $people = $params{people};
    my $aous = $params{aous};
    my $config = $params{config};

    for my $id ( keys %$people ) {
	my $person = $people->{$id};
	my $desc = join(
	    ' ',
	    $person->{StaffID},
	    $person->{Given_Name},
	    $person->{Family_Name}
	    );
	my $aouID = $person->{GroupID_1};

	if( my $aou = $aous->{$aouID} ) {
	    my $mu_code = $aou->{Parent_Group_ID};
	    if( my $url = $config->{$mu_code} ) {
		$url =~ s/\$ID/$person->{SMID}/;
		$person->{Staff_Profile_Homepage} = $url;
	    } else {
		$log->log("[$desc] Unmatched MU code: '$mu_code' for AOU '$aouID'\n");
	    }
	} else {
	    $log->log("[$desc] Unmatched AOU code: '$aouID'\n"); 
	}
    }
}


