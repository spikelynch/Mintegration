#!/usr/bin/perl

use strict;

use Text::CSV;
use Log::Log4perl;
use Data::Dumper;

use Crypt::Skip32;

# This is a quicky to generate an activities file for Mint ingest, and to
# also filter the people file down to only those who have been named on
# a post-2009 projecct with an unique SFOLIO number


Log::Log4perl::init('./log4perl.conf');

my $log = Log::Log4perl->get_logger('projects');

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

my $KEY = '1E23F1709478F63D2083';

my $CUTOFF_YEAR = 2009;


my $UTS_NAME = 'University of Technology, Sydney';


my $BASEURL = '/home/mike/workspace/RDC Mint/test';

my $PROJECTS_IN = "$BASEURL/working/Mint_Project_With_All_CIs.csv";

my $PROJECTS_OUT = "$BASEURL/harvest/Activities.csv";

my $PEOPLE_IN = "$BASEURL/harvest/People_with_URLs.csv";

my $PEOPLE_PROJECTS_OUT = "$BASEURL/harvest/People_Projects.csv";

my $PEOPLE_FILTERED = "$BASEURL/harvest/People_Filtered.csv";


my @OUTHEADS = (
    'ID', 'Submit Year', 'Start Year', 'Title', 'Description',
    'Institution', 'Investigators', 'Discipline' );

$log->info("Reading projects from $PROJECTS_IN");

my $raw_projects = read_projects($PROJECTS_IN);

my $projects = {};
my $people = {};
my $ids = {};

my $tick = 0;

$log->info("Converting projects");

for my $row ( @$raw_projects ) {
    my $id =    $row->[CCODE];

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
        $people->{$id} = {};
        if( my $folio = $row->[SFOLIO] ) {
            $ids->{$folio}{$id} = 1;
        }
    }

    my $name =  $row->[CPERSON_NAME];
    my $sid =   $row->[CPERSON_CODE];
    my $order = $row->[NORDER];

    if( $people->{$id}{$order} ) {
        $log->info("[$id] Multiple sids on same norder");
        my $suffix = 1;
        while ( $people->{$id}{"$order.$suffix"} ) {
            $suffix++;
        }
        $order = "$order.$suffix";
    }
    $people->{$id}{$order} = { name => $name, sid => $sid };
}
    

$log->info("Removing projects that share a SFOLIO");

for my $sfolio ( sort keys %$ids ) {
    if ( scalar( keys %{$ids->{$sfolio}} ) > 1 ) {
        $log->debug("Removing $sfolio: " . join(', ', keys %{$ids->{$sfolio}}));
        for my $id ( keys %{$ids->{$sfolio}} ) {
            delete $projects->{$id};
        }
    }
}

$log->info("Removing projects with no SFOLIO");

for my $id ( keys %$projects ) {
    if( !$projects->{$id}[0] ) {
        $log->debug("Removing $id");
        delete $projects->{$id};
    }
}

my $n = scalar(keys %$projects);

$log->info("Got $n projects");

my $pwp = {};

$log->info("Collating investigators");

for my $id ( sort keys %$projects ) {
    if( my $p = $people->{$id} ) {
        my @names = ();
        for my $order ( sort { $a <=> $b } keys %$p ) {
            my $pp = $p->{$order};
            if( $pp->{name} ) {
                push @names, $pp->{name};
            }
            if( $order == 1 ) {
                if( $pp->{sid} =~ /^\d\d\d\d\d\d$/ ) {
                    if( ! $pwp->{$pp->{sid}} ) {
                        $pwp->{$pp->{sid}} = {
                            name => $pp->{name},
                            projects => []
                        };
                    }
                    push @{$pwp->{$pp->{sid}}{projects}}, $id;
                }
            }
        }
        my $names = join('; ', @names);
        my $disc = pop @{$projects->{$id}};
        push @{$projects->{$id}}, $names, $disc;
    }
}


$log->info("Writing to $PROJECTS_OUT...");

my $csv = Text::CSV->new({eol => $/});

open(my $fh, ">:encoding(utf8)", $PROJECTS_OUT) || do {
    $log->fatal("Couldn't open $PROJECTS_OUT for writing: $!");
    die;
};

$csv->print($fh, \@OUTHEADS);


for my $project ( values %$projects ) {
    $csv->print($fh, $project);
}

close $fh;


$log->info("Got " . scalar(keys %$pwp) . " SIDs in projects");


$log->info("People/Project list to $PEOPLE_PROJECTS_OUT...");




open(my $fh2, ">:encoding(utf8)", $PEOPLE_PROJECTS_OUT) || do {
    $log->fatal("Couldn't open $PEOPLE_PROJECTS_OUT for writing: $!");
    die;
};

$csv->print($fh2, [ 'SID', 'Name', 'Projects' ]);


for my $sid ( keys %$pwp ) {
    my $name = $pwp->{$sid}{name};
    my $projects = $pwp->{$sid}{projects};
    $log->debug("$sid $name $projects");
    $csv->print($fh2, [ $sid, $name, @$projects ]);
}

close $fh2;

my ( $headers, $ppl ) = read_people($PEOPLE_IN);

$log->info("Encrypting keys");

my $cryptids = encrypt_ids(ids => [ keys %$pwp ], key => $KEY);


$log->info("Filtering $PEOPLE_IN by projects");

my $filtered = {};

for my $cid ( keys %$ppl ) {
    my $id = $cryptids->{$cid};
    if( $id && $pwp->{$id} ) {
        $filtered->{$cid} = $ppl->{$cid};
    } else {
        $log->debug("Filtering out $cid/$id");
    }
}

$log->info("Got " . scalar(keys %$filtered) . " filtered staff");

open(my $fh3, ">:encoding(utf8)", $PEOPLE_FILTERED)  || do {
    $log->fatal("Couldn't open $PEOPLE_FILTERED for writing: $!");
    die;
};

$csv->print($fh3, $headers);

for my $id ( keys %$filtered ) {
    $csv->print($fh3, $filtered->{$id});
}

close $fh3;


$log->info("Done.");


sub read_projects {
    my ( $file ) = @_;
    
    my $csv = Text::CSV->new();
    my $fh;
    open($fh, "<:encoding(utf-8)", $file) || die("Couldn't open $file $!");

    my $rows = [];

    while ( my $row = $csv->getline($fh) ) {
        if( substr($row->[DSTART], 0, 4) < $CUTOFF_YEAR ) {
            next;
        }
        push @$rows, $row;
    }

    close $fh;

    return $rows;
}


sub read_people {
    my ( $file ) = @_;
    
    my $csv = Text::CSV->new();
    my $fh;
    open($fh, "<:encoding(utf-8)", $file) || die("Couldn't open $file $!");

    my $header = $csv->getline($fh);

    my $rows = {};

    while ( my $row = $csv->getline($fh) ) {
        my $id = $row->[0];
        $rows->{$id} = $row;
    }

    close $fh;

    return ( $header, $rows );
}



sub encrypt_ids {
	my %params = @_;
	
	my $ids = $params{ids};
	my $key = $params{key};

    my $newids = {};
	
	if( $key !~ /^[0-9A-F]{20}$/ ) {
		$log->error("cryptKey must be a 20-digit hexadecimal number.");
		die("Invalid cryptKey - must be 20-digit hex");
	}
	
	my $keybytes = pack("H20", $key);
	
	my $cypher = Crypt::Skip32->new($keybytes);
	
	for my $id ( @$ids ) {
		
		my $plaintext = pack("N", $id);
		my $encrypted = $cypher->encrypt($plaintext);
		my $new_id = unpack("H8", $encrypted);
        $newids->{$new_id} = $id;
	}
	
	return $newids;
}	
