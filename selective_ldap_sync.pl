#!/usr/bin/perl -w
#

use strict;
use Getopt::Std;
use Net::LDAP;
use Data::Dumper;
$|=1;

sub compare(@);
sub print_usage();
sub save_timestamps(@);
sub get_timestamps(@);
sub get_ou(@);
sub attr_not_unique($$$$);

our @config;
# store timestamps by hostname.  Pull createtimestamp and
# modifytimestamp from each search and store the highest value.  at
# the end of the script run store the value which will be the highest
# create and modify timestamp encountered.  Don't store it sooner,
# otherwise a subsequent search could miss values modified since the
# last run.
my %timestamps;

my %opts;
getopts('dc:nfa', \%opts);
$opts{c} || print_usage();

require $opts{c};

print "starting at ", `date`, "\n"
  if (exists $opts{d} || exists $opts{a});

print "-n used, ldap will not be modifed.\n"
  if (exists $opts{n});
print "-f used, full sync will be performed, latest create and modify timestamps will be ignored\n"
  if (exists $opts{f});
print "-d used, debugging enabled\n"
  if (exists $opts{d});
print "-a used, additional debugging enabled, output will potentially be very large\n"
  if (exists $opts{a});

for my $src (@config) {
    compare (%$src);
}

for my $h (keys %timestamps) {
    save_timestamps($timestamps{$h}{createtimestamp}, $timestamps{$h}{modifytimestamp}, $h, 
		    $timestamps{$h}{ldap}, $timestamps{$h}{binduser});
}

print "\nfinished at ", `date`
  if (exists $opts{d} || exists $opts{a});

sub compare(@) {
   my (%_config) = @_;

   my $host = $_config{host};

   if (!exists $_config{"dest"}) {
       print "no dest for $host in compare, returning..\n";
       return;
   }

   my $ldap_src;
   my $rslt_dest;

   for my $dest (sort keys %{$_config{dest}}) {
       my $uniqueattr_dest;

       if (exists $_config{dest}{$dest}{uniqueattr}) {
	   $uniqueattr_dest = $_config{dest}{$dest}{uniqueattr}
       } else {
	   $uniqueattr_dest = $_config{uniqueattr}
       }

       my $ldap_dest = Net::LDAP->new($dest);
       $rslt_dest = $ldap_dest->bind($_config{dest}{$dest}{binddn}, password => $_config{dest}{$dest}{bindpass});
       $rslt_dest->code && die "unable to bind as ", $_config{dest}{$dest}{binddn}, ": ", $rslt_dest->error;

       my ($modify_time, $create_time) = get_timestamps($ldap_dest, $host, $_config{dest}{$dest}{binddn})
	 if (!exists $opts{f});

       my $rslt_src;
       my $rslt_timestamps_src;

       ## TODO: move out of the dest loop?
       $ldap_src = Net::LDAP->new($host);
       $rslt_src = $ldap_src->bind($_config{binddn}, password => $_config{bindpass});
       $rslt_src->code && die "unable to bind as ", $_config{binddn}, ": ", $rslt_src->error;

       my $filter_src = $_config{filter};
       if (!exists $opts{f}) {
	   if (exists $_config{dest}{$dest}{attrs}) {
	       if (defined $modify_time && $modify_time) {
		   # we're syncing attrs so only interested in modifytimestamp
		   my $search_time = $modify_time+1;
		   $filter_src = "(&" . $filter_src . "(modifytimestamp>=" . $search_time ."Z))";
	       }
	   } elsif (defined $create_time && $create_time) {
	       # we're syncing whole entries so only interested in createtimestamp
	       my $search_time = $create_time+1;
	       $filter_src = "(&" . $filter_src . "(createtimestamp>=" . $search_time ."Z))";
	   }
       }

       if (exists $opts{d} || exists $opts{a}) {
	   print "\n src: ";
	   print "ldap://"
	     if ($host !~ /^ldap[s]*:\/\//i);
	   print $host , "/" , $_config{base} , "?";
	   print join ',', @{$_config{attrs}}
	     if (exists $_config{attrs});
	   print "?";
	   print $_config{scope}
	     if (exists $_config{scope});
	   print "?" , $filter_src;
	   print "\n";
       }

       my $src_scope = "sub";
       $src_scope = $_config{scope}
	 if (exists $_config{scope});

       my $dest_scope = "sub";
       $dest_scope = $_config{dest}{$dest}{scope}
	 if (exists $_config{dest}{$dest}{scope});

       my $attrs_src;
       if (exists $_config{dest}{$dest}{attrs} ) {

	   $attrs_src = [@{$_config{attrs}}, ("modifytimestamp", "createtimestamp", $_config{uniqueattr})];
	   $rslt_src = $ldap_src->search(base => $_config{base}, filter => $filter_src, scope => $src_scope,
	   				 attrs => $attrs_src);
       } else {
	   # if we're adding users and thus asking for all attributes
	   # we need to do a separate search to collect the create and
	   # modify timestamps.

	   if (exists $opts{d} || exists $opts{a}) {
	       print "search to collect modifytimestamp and createtimestamp: ";
	       print "ldap://"
		 if ($host !~ /^ldap[s]*:\/\//i);
	       print $host , "/" , $_config{base} , "?";
	       # print join ',', @{$_config{attrs}}
	       # 	 if (exists $_config{attrs});
	       print "modifytimestamp,createtimestamp";
	       print "?";
	       print $src_scope;
#		 if (exists $_config{scope});
	       #	   print "?" , $_config{filter};
	       print "?" , $filter_src;
	       print "\n";
	   }

	   $rslt_src = $ldap_src->search(base => $_config{base}, filter => $filter_src,);
	   $rslt_timestamps_src = $ldap_src->search(base => $_config{base}, filter => $filter_src, 
						    scope => $src_scope,
						    attrs=>["modifytimestamp", "createtimestamp"]);
       }

       $rslt_src->code && die "problem searching on $host: " . $rslt_src->error;

       # print ldapurl for debugging
       if (exists $opts{d} || exists $opts{a}) {
	   print "dest: ";
	   print "ldap://"
	     if ($dest !~ /^ldap[s]*:\/\//i);
	   print $dest , "/" , $_config{dest}{$dest}{base} , "?";
	   print join ',', @{$_config{dest}{$dest}{attrs}}
	     if (exists $_config{dest}{$dest}{attrs});
	   print "?";
	   print $_config{dest}{$dest}{scope}
	     if (exists $_config{dest}{$dest}{scope});
	   print "?" , $_config{dest}{$dest}{filter};
	   print "\n";
       }

       my $attrs;
       if (exists $_config{dest}{$dest}{attrs}) {
	   $attrs = [@{$_config{dest}{$dest}{attrs}}, 
	   			       ("modifytimestamp", "createtimestamp", $uniqueattr_dest)];

	   $rslt_dest = $ldap_dest->search(base=>$_config{dest}{$dest}{base}, filter=>$_config{dest}{$dest}{filter}, 
					   scope => $dest_scope, attrs => $attrs);
       } else {
	   $rslt_dest = $ldap_dest->search(base=>$_config{dest}{$dest}{base}, filter=>$_config{dest}{$dest}{filter}, 
					   scope => $dest_scope);
       }

       $rslt_dest->code && die "problem searching on $dest, filter /$_config{dest}{$dest}{filter}/ " . $rslt_dest->error;

       my $src_struct = $rslt_src->as_struct;
       my $dest_struct = $rslt_dest->as_struct;

       my %dest_uniqueattr2dn = populate_uniqueattr2dn($uniqueattr_dest, $dest_struct);

       my ($src_timestamps_struct, $dest_timestamps_struct);
       if (!exists $_config{dest}{$dest}{attrs}) {
	   $src_timestamps_struct = $rslt_timestamps_src->as_struct;
       }

       for my $src_dn (keys %$src_struct) {
	   # TODO: figure out netgroups issue.
	   my $next=0;
	   if (exists $_config{exclude}) {
	       for my $e (@{$_config{exclude}}) {
		   $next = 1
		     if ($src_dn =~ /$e/);
	       }
	   }
	   next if ($next);

	   die "non-existant unique attr: /$src_dn/\n", Dumper $src_struct->{$src_dn}
	       if (!exists $src_struct->{$src_dn}->{$_config{uniqueattr}});

	   my $src_unique_attr = (@{$src_struct->{$src_dn}->{$_config{uniqueattr}}})[0];

	   # capture the latest create and modifytimestamp
	   my ($work_mod_time, $work_create_time);

	   if (exists $_config{dest}{$dest}{attrs}) {
	       # we're modifying
	       $work_mod_time = (@{$src_struct->{$src_dn}->{modifytimestamp}})[0];
	       $work_mod_time =~ s/Z$//;
	       if (!defined $timestamps{lc $host} && !defined $timestamps{lc $host}{modifytimestamp}) {
		   $timestamps{$host}{modifytimestamp} = $work_mod_time;
		   $timestamps{lc $host}{ldap} = $ldap_dest;
		   $timestamps{lc $host}{binduser} = $_config{dest}{$dest}{binddn};
	       }
	   } else {
	       # we're creating

	       if (!defined ($src_timestamps_struct->{$src_dn}->{createtimestamp})) {
		   print "no createtimestamp for $src_dn!?\n";

		   print Dumper $src_timestamps_struct;



	       } else {

		   $work_create_time = (@{$src_timestamps_struct->{$src_dn}->{createtimestamp}})[0];
		   $work_create_time =~ s/Z$//;
		   if (!defined $timestamps{lc $host} && !defined $timestamps{lc $host}{createtimestamp}) {
		       $timestamps{$host}{createtimestamp} = $work_create_time;
		       $timestamps{lc $host}{ldap} = $ldap_dest;
		       $timestamps{lc $host}{binduser} = $_config{dest}{$dest}{binddn};
		   }
	       }
	   }

	   if (defined $timestamps{lc $host}) {
	       if (defined $work_create_time) {
		   if ((defined $timestamps{lc $host}{createtimestamp} && 
			$timestamps{lc $host}{createtimestamp} < $work_create_time) ||
		       !defined $timestamps{lc $host}{createtimestamp}) {
		       $timestamps{lc $host}{createtimestamp} = $work_create_time;
		       $timestamps{lc $host}{ldap} = $ldap_dest;
		       $timestamps{lc $host}{binduser} = $_config{dest}{$dest}{binddn};
			 
		   }
	       }
	       if (defined $work_mod_time) {
		   if ((defined $timestamps{lc $host}{modifytimestamp} && 
			$timestamps{lc $host}{modifytimestamp} < $work_mod_time) ||
		      !defined $timestamps{lc $host}{modifytimestamp}) {
		       $timestamps{lc $host}{modifytimestamp} = $work_mod_time;
		       $timestamps{lc $host}{ldap} = $ldap_dest;
		       $timestamps{lc $host}{binduser} = $_config{dest}{$dest}{binddn};
		   }
	       }
	   } 

	   my $found = 0;

	   # look in the dest ldap struct to see if the user is there.
	   my $user_exists_in_dest = 0;  # if $user_exists is not set, consider adding below


	   # for my $dest_dn (keys %$dest_struct) {
	   #     my $dest_unique_attr = (@{$dest_struct->{$dest_dn}->{$_config{uniqueattr}}})[0];

#	       if (lc $src_unique_attr eq lc $dest_unique_attr) {
#		   print "\t/$dest_dn/\n";

#	   print "dest_uniqueattr2dn: \n", Dumper %dest_uniqueattr2dn;

#	   print "checking for $src_unique_attr..\n";
           if (exists $dest_uniqueattr2dn{lc $src_unique_attr}) {
	       my $dest_dn = $dest_uniqueattr2dn{lc $src_unique_attr};
		   $user_exists_in_dest = 1;

		   # if $_config{attrs} exists we're syncing attributes
		   if (exists $_config{attrs}) {
		       
		       my $i=0;

		       my $rdn_update_dn;
		       my $rdn_update;

		       for (@{$_config{attrs}}) {
			   my $l = ""; my $r = "";

			   # what will be written to the dest ldap
			   my @dest_attrs;

			   # print "\tcomparing ", $_config{attrs}->[$i], "\n";

			   # what will be compared against the src ldap to see if an update has to be made to the dest
			   my @dest_attrs_for_compare;
		       
			   # convert DNs from the src ldap to corresponding DNs in the dest ldap
			   if ((lc $_config{attrs}->[$i] eq "uniquemember") || (lc $_config{attrs}->[$i] eq "member") ) {
			       # go through the src ldap and convert the
			       # uniquemembers to DNs in the dest ldap for
			       # writing to dest ldap.

			       my @src_attrs_for_compare;
			       my $updated_rdn = 0;

			       for my $member (@{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}) {
				   print "\nconverting source dn: /$member/..\n"
				     if (exists $opts{a});
				   
				   # get the uid of the member attribute
				   my $member_rslt_src = $ldap_src->search(base => $member, filter => "objectclass=*", 
									   attrs => "uid");
				   if ($member_rslt_src->code) {
				       next if ($member_rslt_src->error eq "No such object");
				       if ($member_rslt_src->error eq "Invalid DN") {
					   print $member_rslt_src->error, " $member for dn $src_dn\n"
						 if (exists $opts{d} || exists $opts{a});
					   next;
				       }
				       die "problem searching while converting uniquemember /$member/: " . $member_rslt_src->error;
				   }
				   my $member_struct_src = $member_rslt_src->as_struct;
				   my $dn = (keys %$member_struct_src)[0];

				   my $uid = @{$member_struct_src->{$dn}->{uid}}[0];
				   print "\tuid: $uid\n"
				     if (exists $opts{a});

				   if (!defined $uid) {
				       print "skipping $dn, no uid associated.\n"
					 if (exists $opts{d} || exists $opts{a});
				       next;
				   }

				   # convert it to the corresponding dn in the dest ldap
				   my $user_base;
				   if (exists ($_config{dest}{$dest}{user_base})) {
				       $user_base = $_config{dest}{$dest}{user_base}
				   } else {
				       $user_base = $_config{dest}{$dest}{base}
				   }
				       
				   my $member_rslt_dest = $ldap_dest->search(base => $user_base,
									     filter => "uid=$uid");
				   if ($member_rslt_dest->code) {
				       next if ($member_rslt_dest->error eq "No such object");
				       die "problem searching dest while converting uniquemembers: ". 
					 $member_rslt_dest->error;
				   }

				   my $member_struct_dest = $member_rslt_dest->as_struct;
				   my $dest_dn = (keys %$member_struct_dest)[0];
				   print "\tdest dn: /$dest_dn/\n"
				     if (exists $opts{a});

				   push @dest_attrs, $dest_dn if defined $dest_dn;

				   # only populate src_attrs_for_compare if the user is found in the dest ldap.
				   push @src_attrs_for_compare, $dn if defined $dest_dn;
			       }

			       # go through the members of the dest group
			       # and convert them to DNs in the src ldap
			       # for comparison.
			       
			       for my $member (@{$dest_struct->{$dest_dn}->{$_config{dest}{$dest}{attrs}->[$i]}}) {
				   #			       print "\nlooking up dest /$member/..\n";
				   my $member_rslt_dest = $ldap_dest->search(base => $member, filter => "objectclass=*", 
									     attrs => "uid");
				   if ($member_rslt_dest->code) {
				       next if ($member_rslt_dest->error eq "No such object");
				       die "problem searching while converting uniquemembers: " . $member_rslt_dest->error;
				   }

				   my $member_struct_dest = $member_rslt_dest->as_struct;
				   my $dn = (keys %$member_struct_dest)[0];

				   my $uid = @{$member_struct_dest->{$dn}->{uid}}[0];
				   #			       print "\t uid: $uid\n";


				   # convert it to the corresponding dn in the dest ldap
				   my $user_base;
				   if (exists $_config{user_base}) {
				       $user_base = $_config{user_base};
				   } else {
				       $user_base = $_config{base};
				   }
				   my $member_rslt_src = $ldap_src->search(base => $user_base,
				   					   filter => "uid=$uid");
				   if ($member_rslt_src->code) {
				       next if ($member_rslt_src->error eq "No such object");
				       die "problem searching dest while converting uniquemembers: ". 
					 $member_rslt_src->error;
				   }
				   my $member_struct_src = $member_rslt_src->as_struct;
				   my $src_dn = (keys %$member_struct_src)[0];
				   push @dest_attrs_for_compare, $src_dn if defined $src_dn;
			       }

			       $l .= " "
				 if ($l !~ "");
			       $r .= " "
				 if ($r !~ "");

			       $r .= join ' ', sort @dest_attrs_for_compare;
			       $l .= join ' ', sort @src_attrs_for_compare;
			
			   } else {
			       $l .= " "
				 if ($l !~ "");
			       $r .= " "
				 if ($r !~ "");

			       $l .= join ' ', sort @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}
				 if (defined($src_struct->{$src_dn}->{$_config{attrs}->[$i]}));

			       $r .= join ' ', sort @{$dest_struct->{$dest_dn}->{$_config{dest}{$dest}{attrs}->[$i]}}
				 if (defined ($dest_struct->{$dest_dn}->{$_config{dest}{$dest}{attrs}->[$i]}));

			       @dest_attrs = sort @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}
				 if (defined ($src_struct->{$src_dn}->{$_config{attrs}->[$i]}));

			   }

			   if (lc $r ne lc $l) {
			       if (attr_not_unique($ldap_src, $_config{base}, $_config{uniqueattr}, $src_unique_attr)) {
				   print "\nmultiple entries have $_config{uniqueattr}=$src_unique_attr!  Skipping.\n";
				   next;
			       }

			       print "\n$src_dn -> $dest_dn";
			       print ", ", $_config{attrs}->[$i], "->", $_config{dest}{$dest}{attrs}->[$i], "\n";

			       # In the case of uniquemember and member
			       # attributes $r is the converted attributes
			       # to what they are in the source ldap.  The
			       # attributes in the destination are printed
			       # in the Dumper of %modify.
			       print "\t/$l/ -> \n\t/$r/\n";

			       # check to see if the current attribute is the rdn
			       # if so check for the new uid, archive (print it to output) it and delete it before performing modrdn
			       if ($dest_dn =~ /^$_config{dest}{$dest}{attrs}->[$i]/) {
				   my $user_base;

				   if (exists $_config{dest}{$dest}{user_base}) {
				       $user_base = $_config{dest}{$dest}{user_base};
				   } else {
				       $user_base = $_config{dest}{$dest}{base};
				   }
			       
				   my $rslt_uid_dest = $ldap_dest->search(base => $user_base, filter => 
						  "uid=" . @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}[0]);
				   $rslt_uid_dest->code && die "problem looking for uid", 
				     @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}[0], " in $dest: ", 
				       $rslt_uid_dest->error. "\n";
				   
				   my $uid_struct = $rslt_uid_dest->as_struct;

				   if ($rslt_uid_dest->count() > 0) {
				       for my $uid_dn (keys %$uid_struct) {
					   print "\ndelete: $uid_dn\n";
					   for my $a (keys %{$uid_struct->{$uid_dn}}) {
					       for my $v (@{$uid_struct->{$uid_dn}{$a}}) {
						   print "$a: $v\n";
					       }
					   }

					   if (!exists $opts{n}) {
					       my $rslt_update_dest = $ldap_dest->delete($uid_dn);
					       $rslt_update_dest->code && die "modify dest ldap failed: ", 
						 $rslt_update_dest->error;
					   }
				       }
				   }

				   $rdn_update_dn = @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}[0];
				   $rdn_update = {dn => $dest_dn,
						  newrdn => "uid=" . @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}[0],
						  deleteoldrdn => "1"
						 };


				   # print "\nupdating rdn: uid=", @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}[0], "\n";

				   # if (!exists $opts{n}) {
				   #     my $rslt_update_dest = $ldap_dest->modrdn (
                                   #         dn => $dest_dn,					      
				   # 	   newrdn => "uid=" . @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}[0],
                                   #         deleteoldrdn => "1"
                                   #     );

				   # $rslt_update_dest->code && die "modify dest ldap failed: ", 
				   #   $rslt_update_dest->error;
			       #}

			       } else {
				   my %modify;

				   $modify{replace} = { $_config{dest}{$dest}{attrs}->[$i] => [ @dest_attrs ] };  

				   print "modify: ", Dumper %modify
				     if (exists ($opts{d}) || exists ($opts{a}));
				   
				   if (!exists $opts{n}) {
				       my $rslt_update_dest = $ldap_dest->modify($dest_dn, %modify);
				       # $rslt_update_dest->code && die "modify dest ldap failed: ", 
				       # 	 $rslt_update_dest->error;

				       if ($rslt_update_dest->code) {
					   if ($rslt_update_dest->error =~ /Another entry with the same attribute value already exists/) {
					       warn "modify dest ldap failed: ", $rslt_update_dest->error;
					   } else {
					       die "modify dest ldap failed: ", $rslt_update_dest->error;
					   }
				       }
				   }
			       }

			   } elsif (exists $opts{a}) {
			       print "\n$src_dn -> $dest_dn";
			       print ", ", $_config{attrs}->[$i], "->", $_config{dest}{$dest}{attrs}->[$i], "\n";
			       print "\t/$l/ -> \n\t/$r/\n";
			   }
			   $i++;
		       }

		       # Let the loop finish to get the attributes updated
		       # before modifying the dn.  Otherwise all future
		       # attribute updates would fail.
		       
		       if (defined $rdn_update && defined $rdn_update_dn) {
		       	   print "\nupdating rdn: uid=", $rdn_update_dn;
		       
		       	   if (!exists $opts{n}) {
		       	       my $rslt_update_dest = $ldap_dest->modrdn (%$rdn_update);

		       	       $rslt_update_dest->code && die "modify dest ldap failed: ", 
		       		 $rslt_update_dest->error;
		       	   }
			   print "\n";
		       }
		   }
	   }

	   if (!$user_exists_in_dest) { 
	       # user does not exist in dest ldap.  Add them if {dest}{add} is set
	       if (exists $_config{dest}{$dest}{add}) {
		   
		   if (!exists $_config{dest}{$dest}{"convert_cmd"}){
		       print "not adding $src_dn, convert_cmd must be defined along with add in dest $dest\n";
		       next;
		   }

		   print "\n\nadding $src_dn to dest $dest:\n";
		   my $entry = "dn: " . $src_dn . "\n";

		   for my $key (keys %{$src_struct->{$src_dn}}) {
		       for (@{$src_struct->{$src_dn}{$key}}) {
			   $entry .= "${key}: $_\n";
		       }
		   }

		   $entry =~ s/\(/\\(/g;
		   $entry =~ s/\)/\\)/g;
		   # TODO: allow quotes 
		   $entry =~ s/\'//g;
		   print "/$entry/\n";

		   my $new_entry = `echo \'$entry\'| $_config{dest}{$dest}{convert_cmd}`;

		   my @new_entry = split (/\n/, $new_entry);
		   my $new_dn = shift @new_entry;
		   $new_dn =~ s/dn:\s*//;

		   my $e = Net::LDAP::Entry->new;
		   print "dn: $new_dn\n";
		   $e->dn($new_dn);
		   for (@new_entry) {
		       my ($attr, $value) = split /:\s*/;
		       # if (lc $attr eq "uniquemember" ||
		       # 	  lc $attr eq "memberuid") {
		       if ((lc $attr eq "uniquemember") || (lc $attr eq "uniquemember")) {

			   # get the uid from the src ldap
#			   print "looking up $value..\n";
			   my $member_rslt_src = $ldap_src->search(base => $value, filter => "objectclass=*", 
								     attrs => "uid");
			   if ($member_rslt_src->code) {
			       next if ($member_rslt_src->error eq "No such object");
			       
			       if ($member_rslt_src->error eq "Invalid DN"){
				   print "Ignoring: $attr: $value, " . $member_rslt_src->error, "\n";
				   next;
			       }
			   }
			   my $member_struct_src = $member_rslt_src->as_struct;
			   my $dn = (keys %$member_struct_src)[0];

			   my $uid = @{$member_struct_src->{$dn}->{uid}}[0];


			   # lookup the dn in the dest ldap
#			   print "\t uid: $uid\n";

			   # convert it to the corresponding dn in the dest ldap
			   my $user_base;
			   if (exists $_config{dest}{$dest}{user_base}) {
			       $user_base = $_config{dest}{$dest}{user_base};
			   } else {
			       $user_base = $_config{dest}{$dest}{base};
			   }
			   # my $member_rslt_dest = $ldap_dest->search(base => $_config{dest}{$dest}{base},
			   # 					     filter => "uid=$uid");
			   my $member_rslt_dest = $ldap_dest->search(base => $user_base,
								     filter => "uid=$uid");
			   if ($member_rslt_dest->code) {
			       next if ($member_rslt_dest->error eq "No such object");
			       die "problem searching dest while converting uniquemembers: ". 
				 $member_rslt_dest->error;
			   }

			   my $member_struct_dest = $member_rslt_dest->as_struct;
			   my $dest_dn = (keys %$member_struct_dest)[0];
#			   print "\tdest dn: /$dest_dn/\n";
			   $value = $dest_dn
		       }
		       if (defined($value)) {
			   print "$attr: $value\n";
			   $e->add ($attr => $value);
		       } else {
			   print "skipping $attr, value is emtpy.\n";
		       }
		   }
		   
		   if (!exists $opts{n}) {
		       my $dest_add_rslt =  $ldap_dest->add($e);
		       $dest_add_rslt->code && 
			 warn "problem adding: ". $dest_add_rslt->error;
		   }
	       }
	   }
       }
   }
}




# save the latest timestamps in the description field of the dest sync user
# this won't work if the user is directory manager of course.
sub save_timestamps(@) {
    my ($ct, $mt, $src_host, $ldap, $binduser) = @_;

    # if (!defined ($ct) || !defined ($mt) ||
    #    !$ct || !$mt) {
    # 	if (exists ($opts{d}) || exists($opts{a})) {
    # 	    print "\ncreate time or modify time not returned from ldap.\nThis usually means the source search filter returned no results.\n";
    # 	}
    # 	    return;
    # }

#     print "latest create time: /$ct/\n";
#     print "latest modify time: /$mt/\n";

    my %modify;

    my $rslt = $ldap->search(base=>$binduser, filter=>"objectclass=*");
    $rslt->code && die "search failed in save_timestamps: " . $rslt->error;

    my $e = $rslt->as_struct;

    my $dn = (keys (%$e))[0];
    my @description;
    if (exists $e->{$dn}->{description}) {
	@description = @{$e->{$dn}->{description}};
    }

    my ($new_ct, $new_mt);
    my (@create_desc, @modify_desc);

    if (@description) {
	@create_desc = grep (/createtimestamp;$src_host/, @description);
	# replace one or more descriptions if applicable
	if (@create_desc) {
	    for my $d (@create_desc) {
		($d) = ($d =~ /^[^;]+;$src_host;\s*(.*)/);
		if (defined $ct && $ct > $d) {
		    update_modify(\%modify, "createtimestamp", $src_host, $ct, $d);
		}
	    }
	} else {
	    update_modify(\%modify, "createtimestamp", $src_host, $ct);
	}

	@modify_desc = grep (/modifytimestamp;$src_host/, @description);
	# replace one or more descriptions if applicable
	if (@modify_desc) {
	    for my $d (@modify_desc) {
		($d) = ($d =~ /^[^;]+;$src_host;\s*(.*)/);
		if (defined $mt && $mt > $d) {
		    update_modify(\%modify, "modifytimestamp", $src_host, $mt, $d);
		}
	    }
        } else {
	    update_modify(\%modify, "modifytimestamp", $src_host, $mt);
	}
    } else {
	update_modify(\%modify, "createtimestamp", $src_host, $ct);
	update_modify(\%modify, "modifytimestamp", $src_host, $mt);
    }
    
    # update the description of the bind user if modifies were generated
    if (%modify) {
	print "\nupdating timestamps: \n", Dumper %modify
	    if ((exists $opts{d} || exists $opts{a}));

	if (!exists $opts{n}) {
	    my $rslt2 = $ldap->modify($binduser, %modify);
	    $rslt2->code && die "modify failed in save_timestamp: ", $rslt2->error;
	}
    }






}


{

# only return create and modify timestamps once per session otherwise
# with multiple source host entries the first entry will keep subsequent entries from doing
# proper searches.

my %previous_hosts;

sub get_timestamps(@) {
    my ($ldap, $src_host, $binduser) = @_;

    return @{$previous_hosts{lc $src_host}} if (exists $previous_hosts{lc $src_host});

    my $rslt = $ldap->search(base=>$binduser, filter=>"objectclass=*");
    $rslt->code && die "search failed in save_timestamps: " . $rslt->error;
    
    my $e = $rslt->as_struct;

    my $dn = (keys (%$e))[0];
    my (@description, $ct, $mt);
    if (exists $e->{$dn}->{description}) {
	@description = @{$e->{$dn}->{description}};
    }

    if (@description) {
	my @create_desc = grep (/createtimestamp;$src_host/, @description);
	# replace one or more descriptions if applicable
	if (@create_desc) {
	    for my $d (@create_desc) {
		($ct) = ($d =~ /[^;]+;$src_host;\s*(.*)/);
	    }
	}

	my @modify_desc = grep (/modifytimestamp;$src_host/, @description);
	# replace one or more descriptions if applicable
	if (@modify_desc) {
	    for my $d (@modify_desc) {
		($mt) = ($d =~ /[^;]+;$src_host;\s*(.*)/);
	    }
	}
    }

    @{$previous_hosts{lc $src_host}} = ($mt, $ct);
    
    return ($mt, $ct);
}
}


sub print_usage() {
    print "\nusage: $0 [-n] [-f] [-d[d]] -c <config file>\n";
    print "\t-n just print: don't make changes\n";
    print "\t-f full sync: ignore saved timestamps\n";
    print "\t-d print debugging\n";
    print "\t-a print additional debugging, about three lines of output for every entry in your source directory\n";
    print "\n\n";
    exit;
}


sub update_modify (@) {
    my ($mod_ref, $type, $src_host, $new_val, $old_val) = @_;
#    print "update_modify called with /$type/ /$new_val/ /$old_val/\n";
    
    return if (!defined $old_val);

    if (!defined $old_val) {
	# just add
	if (exists $$mod_ref{add}) {
	    push @{$mod_ref->{add}->{description}}, "$type;$src_host;$new_val";
	} else {
	    $$mod_ref{add} = {description => ["$type;$src_host;$new_val"]};
	}
    } else {

	if (exists $$mod_ref{delete}) {
	    push @{$mod_ref->{delete}->{description}}, "$type;$src_host;$old_val";
	} else {
	    $$mod_ref{delete} = {description => ["$type;$src_host;$old_val"]};
	}

	if (exists $$mod_ref{add}) {
	    push @{$mod_ref->{add}->{description}}, "$type;$src_host;$new_val";
	} else {
	    $$mod_ref{add} = {description => ["$type;$src_host;$new_val"]};
	}
    }
}


sub get_ou(@) {
    my $dn = shift;

    return (split /\s*,\s*/, $dn)[1];
}

sub populate_uniqueattr2dn {
    my ($unique_attr_name, $ldap_struct) = @_;

    my %uniqueattr2dn;

    $unique_attr_name = lc $unique_attr_name;

    for my $dn (keys %$ldap_struct) {
#	print "dn: $dn, uniqueattr: $unique_attr_name\n";
	my $unique_attr = (@{$ldap_struct->{$dn}->{$unique_attr_name}})[0];

	$uniqueattr2dn{lc $unique_attr} = $dn;
    }

    return %uniqueattr2dn;
}


sub attr_not_unique($$$$) {
    my ($ldap, $base, $attr, $value) = @_;

    my $rslt = $ldap->search(base => $base, filter => $attr . "=" . $value);

    $rslt->code && die "problem with search in attr_not_unique: ", $rslt->error, "\n";

    return 1
      if ($rslt->count() > 1);
    
    return 0;
}
