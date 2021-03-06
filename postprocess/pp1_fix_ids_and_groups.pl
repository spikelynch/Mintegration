#!/usr/bin/perl

=head1 NAME

pp1_fix_ids_and_groups.pl

=head1 SYNOPSIS

  [... java app retrieves the raw CSV from staff module... ]

  ./pp1_fix_ids_and_groups.pl -c mintIntConfig.xml
  ./pp2_test_profile_urls.pl -c mintIntConfig.xml


=head1 DESCRIPTION

A script to perform the following data-cleaning operations on the
feeds for parties (people and groups) after they are fetched from the
Staff Module and before they are harvested by Mint:

=over 4

=item Encrypt staff numbers to use as an ID

=item Throw away old AOUs marked "DO NOT USE"

=item Generate staff profile URLs

=back


=head1 CONFIGURATION

=head2 Environment variables

If any of these is missing, the script won't run:

=over 4

=item MINT_PERLLIB - location of MintUtils.pm

=item COATAGLUE_PERLLIB - location of the CoataGlue library

=item MINT_CONFIG - location of the Mint/RDC config XML file

=item MINT_LOG4J - location of the log4j.properties file

=back

=head2 Command-line switches

=over 4

=item -c CONFIGFILE - config file (overrides MINT_CONFIG above)

=item -n            - Don't do live URL tests

=item -h            - Print help

=item -i ID         - encrypt a single staff ID and write to STDOUT


=back

=cut

use strict;

if( ! $ENV{MINT_PERLLIB} || ! $ENV{MINT_CONFIG} || ! $ENV{MINT_LOG4J}) {
	die("One or more missing environment variables.\nRun perldoc $0 for more info.\n");
}


use lib $ENV{MINT_PERLLIB};
use lib $ENV{COATAGLUE_PERLLIB};

use Data::Dumper;
use Getopt::Std;
use Log::Log4perl;
use Crypt::Skip32;


use MintUtils qw(read_csv read_mint_cfg write_csv);

my $DEFAULT_CONFIG = 'config.xml';
my $LOGGER = 'mintInt.clean';

my %opts = ();

getopts("c:i:h", \%opts) || usage();

if( $opts{h} ) {
    usage();
}

die("Need to point MINT_LOG4J to the log4j.properties file\n") unless $ENV{MINT_LOG4J};

Log::Log4perl::init($ENV{MINT_LOG4J});

my $log = Log::Log4perl->get_logger($LOGGER);

my $config = $opts{c} || $ENV{MINT_CONFIG} || $DEFAULT_CONFIG;


my $mint_cfg   = read_mint_cfg(file => $config) || do {
	$log->fatal("Configuration error.");
	exit(1);
};

$log->trace(Dumper({ config => $mint_cfg }));

if( my $id = $opts{i} ) {
    my $eid = encrypt_one_id(ptid => $id, config => $mint_cfg->{staffIDs});
    print "$id => $eid\n";
    exit;
}



my $working_dir = $mint_cfg->{locations}{working} || './';
my $harvest_dir = $mint_cfg->{locations}{harvest} || './';


$working_dir .= '/' unless $working_dir =~ m#/$#;
$harvest_dir .= '/' unless $harvest_dir =~ m#/$#;

$log->debug("Working dir $working_dir");
$log->debug("Harvest dir $harvest_dir"); 

$log->info("Reading raw people file");

my $people = read_csv(
    config => $mint_cfg,
    dir => $working_dir,
    query => 'People',
    file => 'raw'
);

$log->info("Reading raw groups file");

my $aous = read_csv(
    config => $mint_cfg,
    dir => $working_dir,
    query => 'Groups',
    file => 'raw'
);

$log->info("Reading raw MU file");


my $mus = read_csv(
    config => $mint_cfg,
    dir => $working_dir,
    query => 'ManagingUnits',
    file => 'raw'
);


for my $muid ( keys %$mus ) {
    if( $aous->{$muid} ) {
        $log->warn("MU/AOU key clash: $muid\n");
    } else {
        $aous->{$muid} = $mus->{$muid}
    }
}

$log->info("Cleaning AOUs");

clean_aous(
    aous => $aous,
    config => $mint_cfg->{groupTidy}
);

$log->info("Building staff URLs");

make_urls(
    aous => $aous,
    people => $people,
    config => $mint_cfg->{landingPageURLs}
);

$log->info("Encrypting staff IDs");

my $reindexed = encrypt_ids(
	people => $people,
	config => $mint_cfg->{staffIDs}
);

my $ordered_groups = sort_groups_hierarchically(groups => $aous);

$log->info("Writing groups file");

write_csv(
    config => $mint_cfg,
    dir => $harvest_dir,
    query => 'Groups',
    file => 'harvest',
    sortby => $ordered_groups,
    records => $aous
);

$log->info("Writing encrypted staff file");

write_csv(
    config => $mint_cfg,
    dir => $working_dir,
    query => 'People',
    file => 'encrypted',
    records => $reindexed
    );

$log->info("Done.\n");

=head1 SUBROUTINES

=over 4

=item usage()

Prints a command-line help message.

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

Environment variables:

MINT_PERLLIB 	  - location of the MintUtils.pm library
COATAGLUE_PERLLIB - location of the CoataGlue libraries
MINT_CONFIG       - location of the Mint/RDC config XML file
MINT_LOG4J        - location of the log4j.properties file

All of the Mint integration code uses the Log4j logging 
framework (or its Perl emulator) so that logging can be controlled
in a single config gile.  This script\'s logging id is
'$LOGGER'.

EOTXT
exit(0);


}


 
=item clean_aous(aous => $aous)

Clean the aou records: remove faculty prefixes, drop any aous marked
"DO NOT USE" and any other tidying.

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
          $log->info("Ignoring AOU $aouID $name\n");
          delete $aous->{$aouID};
          next AOU;
      }
      
      if( $name =~ /\|/ ) {
          ( $name ) = split(/\|/, $name);
          $log->warn("Split AOU with |: '$name'");
      }
      
      my $parent = $aou->{Parent_Group_ID};
      
      if( $parent && !$aous->{$parent} ) {
          $log->warn("AOU $aouID - $name parent group ID $parent not found.\n");
      } else {
          if( $name =~ /^RS/ || $name =~ /Associate|Member|Core/ ) {
              # For now, delete Research Strength AOUs, because the
              # query that generates the raw People file is not linking 
              # to them.
              
              $log->debug("Removing RS '$name'");
              delete $aous->{$aouID};use Data::Dumper;
              use Template;
              
              next AOU;
          }
          
          # Make sure that all divisions with a Faculty parent
          # get a standardised Faculty prefix
          
          if( $name =~ /^([A-Za-z ]+)\.(.*)$/ ) {
              $log->debug("Dropping prefix $1 from $2");
              $name = $2;
          }
      }
      $aou->{Name} = $name;
  }
}






=item make_urls(people => $people, aous => $aous, config => $config)

Updated staff profile URL builder which uses their email prefix as in:

$FIRSTNAME.$LASTNAME@uts.edu.au => www.uts.edu.au/staff/$FIRSTNAME.$LASTNAME


Tries to make staff profile URLs for each record in people, by matching
them against an AOU, looking for that AOU's parent faculty, and then filling
out a URL from the config file.

If a URL can't be made (because the staff member doesn't belong to an AOU
which can be connected to a faculty) they stay in the data but a warning
is sent to STDERR.


=cut


sub make_urls {
    my %params = @_;

    my $people = $params{people};
    my $aous = $params{aous};
    my $config = $params{config};
    
    my $urlIDfield = $config->{urlID};
    
  PERSON: for my $id ( keys %$people ) {
		my $person = $people->{$id};
		my $urlID = $person->{$urlIDfield};
		if( ! $urlID ) {
			$log->warn("Person record $id without urlID ($urlIDfield) - skipping");
			next PERSON;
		}
		my $desc = join(
	    	' ',
	    	$person->{StaffID},
	    	$person->{Given_Name},
	    	$person->{Family_Name}
	    );
		my $aouID = $person->{GroupID_1};

		if( my $aou = $aous->{$aouID} ) {
		    my $mu_code = $aou->{Parent_Group_ID};
	    	if( my $url = $config->{faculties}{$mu_code} ) {
				$url =~ s/\$ID/$urlID/;
				$person->{Staff_Profile_Homepage} = $url;
	    	} else {
				$log->warn("[$desc] Unmatched MU code: '$mu_code' for AOU '$aouID'\n");
	    	}
		} else {
	    	$log->warn("[$desc] Unmatched AOU code: '$aouID'\n"); 
		}
    }
}



=item encrypt_ids(people => $people, key => $key)

Encrypts the staff IDs to generate a unique, obfuscated hex integer
which doesn't depend on anything apart from the staff ID and our
encryption key.  This is done to provide an identifier to RDA which
doesn't expose the staff ID, but which doesn't depend on any other
system (like the ID numbers in the staff module).

$people is a hashref of people. $key is the value in each person to 
use as the hash.

Returns a hashref with the new, encrypted IDs as keys.

Uses the Crypt::Skip32 block cypher.

=back

=cut

# FIXME - use CoataGlue::Person to do the encryption.


sub encrypt_ids {
	my %params = @_;
	
	my $people = $params{people};
	my $key = $params{config}{cryptKey};
	my $original_id = $params{config}{originalID};
	
	my $reindexed = {};
	
	if( $key !~ /^[0-9A-F]{20}$/ ) {
		$log->error("cryptKey must be a 20-digit hexadecimal number.");
		die("Invalid cryptKey - must be 20-digit hex");
	}
	
	my $keybytes = pack("H20", $key);
	
	my $cypher = Crypt::Skip32->new($keybytes);
	
	for my $id ( keys %$people ) {
		my $person = $people->{$id};
		
		my $plaintext = pack("N", $id);
		my $encrypted = $cypher->encrypt($plaintext);
		my $new_id = unpack("H8", $encrypted);
		$log->debug("Encrypted $id to $new_id");
		$reindexed->{$new_id} = $person;
		$reindexed->{$new_id}{ID} = $new_id;
		delete $reindexed->{$new_id}{$original_id};
	}
	
	return $reindexed;
}	


=item encrypt_one_id(ptid => $id, config => $config)

Encrypts a single staff ID for the -i flag, returning the encrypted string.

=cut




sub encrypt_one_id {
	my %params = @_;
	
    my $ptid = $params{ptid};
	my $key = $params{config}{cryptKey};
	
	my $reindexed = {};
	
	if( $key !~ /^[0-9A-F]{20}$/ ) {
		$log->error("cryptKey must be a 20-digit hexadecimal number.");
		die("Invalid cryptKey - must be 20-digit hex");
	}
	
	my $keybytes = pack("H20", $key);
	
	my $cypher = Crypt::Skip32->new($keybytes);
	
    my $plaintext = pack("N", $ptid);
    my $encrypted = $cypher->encrypt($plaintext);
    my $new_id = unpack("H8", $encrypted);
	return $new_id;
}	


=item sort_groups_hierarchically(groups => $groups)

Takes the groups (which are a flat list of records with links to parent
groups) and sorts them into a list where every group's parents are
guaranteed to occur earlier in the list.

=cut

sub sort_groups_hierarchically {
	my %params = @_;
	
	my $groups = $params{groups};
	my @roots = ();
	
	for my $id ( keys %$groups ) {
		my $parent = $groups->{$id}{Parent_Group_ID};
		if( $parent ) {
			if( $groups->{$parent} ) {
				push @{$groups->{$parent}{children}}, $groups->{$id};
			} else {
				$log->warn("Group $id: Parent_Group_ID '$parent' not found");
			}
		} else {
			push @roots, $groups->{$id}
		}
	}
	
	my $ordered = [];
	
	for my $root ( @roots ) {
		groups_descend(node => $root, keys => $ordered);
	}
	return $ordered;
}





sub groups_descend {
	my %params = @_;
	
	my $node = $params{node};
	my $keys = $params{keys};
	
	$log->trace("Rec descend node $node->{ID} $node->{Name}");
	
	push @$keys, $node->{ID};
	
	for my $child ( @{$node->{children}} ) {
		groups_descend(node => $child, keys => $keys);
	}
	
}
