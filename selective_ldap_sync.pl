#!/usr/bin/perl -w
#

use strict;
use Getopt::Std;
use Net::LDAP;
use Data::Dumper;
$|=1;

sub compare(@);
sub print_usage();
sub save_timestamps();
sub get_timestamps(@);
sub get_ou(@);
sub attr_not_unique($$$$);
sub check_timestamps;
sub get_attr_values(@);
sub attr_has_dependencies;
sub add_to_modify;

our @config;

my %opts;
getopts('dc:nfa', \%opts);
$opts{c} || print_usage();

require $opts{c};

print "starting at ", `date`, "\n"
  if (exists $opts{d} || exists $opts{a});

print "-n used, ldap will not be modified.\n"
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

save_timestamps();

print "\nfinished at ", `date`
  if (exists $opts{d} || exists $opts{a});

sub compare(@) {
   my (%_config) = @_;

   my $host = $_config{host};

   if (!exists $_config{"dest"}) {
       print "no dest for $host in compare, returning..\n";
       return;
   }

   # bind to source
   my $ldap_src = Net::LDAP->new($host);
   my $rslt_src = $ldap_src->bind($_config{binddn}, password => $_config{bindpass});
   $rslt_src->code && die "unable to bind as ", $_config{binddn}, ": ", $rslt_src->error;

   for my $dest (sort keys %{$_config{dest}}) {
       # use the dest uniqueattr if there is one, otherwise the source
       # uniqueattr will be assumed for both src and dest.
       my $uniqueattr_dest;
       if (exists $_config{dest}{$dest}{uniqueattr}) {
	   $uniqueattr_dest = $_config{dest}{$dest}{uniqueattr}
       } else {
	   $uniqueattr_dest = $_config{uniqueattr}
       }

       my $ldap_dest = Net::LDAP->new($dest);
       my $rslt_dest = $ldap_dest->bind($_config{dest}{$dest}{binddn}, 
				     password => $_config{dest}{$dest}{bindpass});
       $rslt_dest->code && die "unable to bind as ", $_config{dest}{$dest}{binddn}, 
	 ": ", $rslt_dest->error;

       my %common_values_to_ignore = get_common_values($ldap_dest, %{$_config{dest}{$dest}});

       my ($modify_time, $create_time) = get_timestamps($ldap_dest, $host, 
					   $_config{dest}{$dest}{binddn})
	 if (!exists $opts{f});

       my $rslt_src;
       my $rslt_timestamps_src;

       my $filter_src = $_config{filter};
       if (!exists $opts{f}) {
	   # dest attrs are used for source search.  I am pretty sure
	   # that is intentional.  If not moving this above the loop
	   # will be a timesaver if there are multiple destinations.
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

       # assume sub scope unless defined otherwise in the config
       my $src_scope = "sub";
       $src_scope = $_config{scope}
	 if (exists $_config{scope});

       # my $src_attrs = [@{$_config{dest}{$dest}{attrs}}, 
       # 		   ("modifytimestamp", "createtimestamp", $_config{uniqueattr})];
       my $src_attrs = [("modifytimestamp", "createtimestamp")];
       if (exists $_config{dest}{$dest}{attrs}) {
	   push @$src_attrs, @{$_config{dest}{$dest}{attrs}}, $_config{uniqueattr}
       }
       if (exists $_config{uniqueattr}) {
	   push @$src_attrs, $_config{uniqueattr};
       }
	 

       # print src ldapurl for debugging
       if (exists $opts{d} || exists $opts{a}) {
       	   print "\n src: ";
       	   print "ldap://"
       	     if ($host !~ /^ldap[s]*:\/\//i);
       	   print $host , "/" , $_config{base} , "?";
       	   print join ',', @$src_attrs
       	     if (exists $_config{attrs});
       	   print "?";
	   print $src_scope;
       	   print "?" , $filter_src;
       	   print "\n";
       }

       my $attrs;
       if (exists $_config{attrs}) {
	   $rslt_src = $ldap_src->search(base => $_config{base}, filter => $filter_src, 
					 attrs=>$src_attrs);
       } else {
	   $rslt_src = $ldap_src->search(base => $_config{base}, filter => $filter_src);
       }
 
       # explicitly request modifytimestamp & createtimestamp if attrs are requested
       my $src_timestamps_struct;
       if (!exists $_config{dest}{$dest}{attrs} ) {
	   # if attrs were not specified we're asking for all attributes
	   # we need to do a separate search to collect the create and
	   # modify timestamps.
	   # TODO: createtimestamp and modifytimestamp are not included in ldapurl?

	   if (exists $opts{d} || exists $opts{a}) {
	       print "search to collect modifytimestamp and createtimestamp: ";
	       print "ldap://"
		 if ($host !~ /^ldap[s]*:\/\//i);
	       print $host , "/" , $_config{base} , "?";
	       print "modifytimestamp,createtimestamp";
	       print "?";
	       print $src_scope;
	       print "?" , $filter_src;
	       print "\n";
	   }

	   $rslt_timestamps_src = $ldap_src->search(base => $_config{base}, filter => $filter_src, 
						    scope => $src_scope,
						    attrs=>["modifytimestamp", "createtimestamp"]);
	   $rslt_src->code && die "problem searching on $host: " . $rslt_src->error;

	   $src_timestamps_struct = $rslt_timestamps_src->as_struct;
       }

       my $src_struct = $rslt_src->as_struct;

       my $dest_scope = "sub";
       $dest_scope = $_config{dest}{$dest}{scope}
	 if (exists $_config{dest}{$dest}{scope});

       my $dest_attrs = [("modifytimestamp", "createtimestamp", "objectclass")];

       if (exists $_config{dest}{$dest}{attrs}) {
	   push @$dest_attrs, @{$_config{dest}{$dest}{attrs}};
       }

       if (exists $_config{dest}{$dest}{uniqueattr}) {
	   push @$dest_attrs, $_config{dest}{$dest}{uniqueattr};
       } elsif (exists $_config{uniqueattr}) {
	   push @$dest_attrs, $_config{uniqueattr};
       }

       # print dest ldapurl for debugging
       if (exists $opts{d} || exists $opts{a}) {
       	   print "dest: ";
       	   print "ldap://"
       	     if ($dest !~ /^ldap[s]*:\/\//i);
       	   print $dest , "/" , $_config{dest}{$dest}{base} , "?";
       	   print join ',', @$dest_attrs
       	     if (exists $_config{dest}{$dest}{attrs});
       	   print "?";
	   print $dest_scope;
       	   print "?" , $_config{dest}{$dest}{filter};
       	   print "\n";
       }

       # don't search by create/modifytimestamp as we need a full
       # result set on the dest to compare with the source.
       if (exists $_config{dest}{$dest}{attrs}) {
	   $rslt_dest = $ldap_dest->search(base=>$_config{dest}{$dest}{base}, 
               filter=>$_config{dest}{$dest}{filter}, scope => $dest_scope, attrs => $dest_attrs);
       } else {
	   $rslt_dest = $ldap_dest->search(base=>$_config{dest}{$dest}{base}, 
               filter=>$_config{dest}{$dest}{filter}, scope => $dest_scope);
       }

       $rslt_dest->code &&
	   die "problem searching on $dest, filter /$_config{dest}{$dest}{filter}/ " . 
	     $rslt_dest->error;


       my $dest_struct = $rslt_dest->as_struct;

       # build a hash of unique attributes correlated with associated DNs
       my %dest_uniqueattr2dn = populate_uniqueattr2dn($uniqueattr_dest, $dest_struct);

       for my $src_dn (keys %$src_struct) {
	   my $next=0;
	   if (exists $_config{exclude}) {
	       for my $e (@{$_config{exclude}}) {
		   $next = 1
		     if ($src_dn =~ /$e/i);
	       }
	   }
	   next if ($next);

	   die "dn does not have a unique attr!? /$src_dn/\n", Dumper $src_struct->{$src_dn}
	       if (!exists $src_struct->{$src_dn}->{$_config{uniqueattr}});

	   my $src_unique_attr = (@{$src_struct->{$src_dn}->{$_config{uniqueattr}}})[0];

	   check_timestamps(\%_config, $src_struct, $dest, $src_dn, $src_timestamps_struct, 
			    $ldap_dest);

	   my $found = 0;

	   # look in the dest ldap struct to see if the user is there.
	   my $user_exists_in_dest = 0;  # if $user_exists is not set, consider adding below

           if (exists $dest_uniqueattr2dn{lc $src_unique_attr}) {
	       my $dest_dn = $dest_uniqueattr2dn{lc $src_unique_attr};
	       $user_exists_in_dest = 1;

	       # if $_config{attrs} exists we're syncing attributes
	       my @mods;
	       if (exists $_config{attrs}) {
		   my $i=0;

		   my $rdn_update_dn;
		   my $rdn_update;

		   for (@{$_config{attrs}}) {
		       my ($l, $r, $da, $dar, $sa, $common_values) = 
			 get_attr_values(\%_config, $src_struct, $dest_struct, 
					 $ldap_src, $ldap_dest, $i, $src_dn, $dest, 
					 $dest_dn, \%common_values_to_ignore);
		       my @dest_attrs = @$da;
		       my @dest_attrs_to_replace = @$dar;

		       # print $_config{dest}{$dest}{attrs}->[$i], " ", join ' ', @dest_attrs, @dest_attrs_to_replace, "\n\n";

		       if (lc $r ne lc $l) {
			   if (attr_not_unique($ldap_src, $_config{base}, $_config{uniqueattr}, $src_unique_attr)) {
			       print "\nmultiple entries have $_config{uniqueattr}=$src_unique_attr!  Skipping.\n";
			       next;
			   }

			   # print "attr: $_\n";
			   # print "da: \n", Dumper $da;
			   # print "dar: \n", Dumper $dar;
			   # print "sa: \n", Dumper $sa;
			   # print "common_values: \n", Dumper $common_values;

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
			       
			       #TODO: This seems to assume uid is the rdn.
			       my $rslt_uid_dest = $ldap_dest->search(base => $user_base, filter => 
								      "uid=" . @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}[0]);
			       $rslt_uid_dest->code && die "problem looking for uid", 
				 @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}[0], " in $dest: ", 
				 $rslt_uid_dest->error. "\n";
				   
			       my $uid_struct = $rslt_uid_dest->as_struct;

			       if ($rslt_uid_dest->count() > 0) {
				   for my $uid_dn (keys %$uid_struct) {
				       print "\ndelete: /$uid_dn/\n";
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
			   } else {


			       
			       if (!exists($common_values_to_ignore{$_config{dest}{$dest}{attrs}->[$i]})) {
				   my %modify;

				   if (my ($dependency, @dependencies) =
				       attr_has_dependencies($_config{dest}{$dest}{attrs}->[$i], 
							     $_config{dest}{$dest}{attr_oc_dependencies})) {
				       #   print "attr ", $_config{dest}{$dest}{attrs}->[$i], " depends on $dependency!\n";
				       add_to_modify(\@mods, $_config{dest}{$dest}{attrs}->[$i],
						     \@{$dest_struct->{$dest_dn}->{objectclass}}, \@dest_attrs,
						     $dependency, \@dependencies);
				   } else {
				       $modify{replace} = { $_config{dest}{$dest}{attrs}->[$i] => [ @dest_attrs ] };  
				       push @mods, \%modify;
				   }
			       } else {
				   if ($_config{dest}{$dest}{attrs}->[$i] =~ /userpassword/i) {
				       # if password history is
				       # enabled both userpassword and
				       # passwordHistory have to be
				       # wiped and new userpassword
				       # has to be added anew.
				       my %modify_del;
#				       $modify_del{delete} = ['passwordHistory', 'userpassword'];
#				       push @mods, \%modify_del;

				       push @mods, {'delete' => 'passwordHistory'};
				       push @mods, {'delete' => 'userpassword'};

				       my @PWs;
				       push @PWs, @$da;
				       push @PWs, @$common_values;
#				       my %modify_add;
				       # $modify_add{add} = {"userpassword" => [@PWs]};
				       # push @mods, \%modify_add;

				       for my $pw (@PWs) {
					 # $modify_add{add} = {"userpassword" => [$pw]};
					 # push @mods, \%modify_add;
					 push @mods, {"add" => {"userpassword" => [$pw]}};
				       }
				       
				   } else {
				   # In the case where one value matches it's important the delete be done before the add.
				       my %modify;
				       if (@dest_attrs_to_replace) {
					   $modify{delete} = {$_config{dest}{$dest}{attrs}->[$i] => [@dest_attrs_to_replace]};
					   push @mods, \%modify;
				       }
				       if (@dest_attrs) {
					   my %modify1;
					   $modify1{add} = {$_config{dest}{$dest}{attrs}->[$i] => [@dest_attrs]};
					   push @mods, \%modify1;
				       }
				   }
			       }

			       # print "modify: ", Dumper @mods;
				   
			       # if (!exists $opts{n}) {
			       #     for my $modify (@mods) {

			       # 	   my $rslt_update_dest = $ldap_dest->modify($dest_dn, %$modify);
			       # 	   # $rslt_update_dest->code && die "modify dest ldap failed: ", 
			       # 	   # 	 $rslt_update_dest->error;

			       # 	   if ($rslt_update_dest->code) {
			       # 	       if ($rslt_update_dest->error =~ /Another entry with the same attribute value already exists/) {
			       # 		   warn "modify dest ldap failed: ", $rslt_update_dest->error;
			       # 	       } else {
			       # 		   die "modify dest ldap failed: ", $rslt_update_dest->error;
			       # 	       }
			       # 	   }
			       #     }
			       # }
			   }

		       } elsif (exists $opts{a}) {
			   print "\n$src_dn -> $dest_dn";
			   print ", ", $_config{attrs}->[$i], "->", $_config{dest}{$dest}{attrs}->[$i], "\n";
			   print "\t/$l/ -> \n\t/$r/\n";
		       }
		       $i++;
		   }


		   print "modify: ", Dumper @mods
		     if (@mods);
		   if (!exists $opts{n}) {
		       for my $modify (@mods) {
			   my $rslt_update_dest = $ldap_dest->modify($dest_dn, %$modify);
			   # $rslt_update_dest->code && die "modify dest ldap failed: ", 
			   # 	 $rslt_update_dest->error;

			   if ($rslt_update_dest->code) {
			       if ($rslt_update_dest->error =~ /Another entry with the same attribute value already exists/ ||
				  $rslt_update_dest->error =~ /No such attribute/) {
				   warn "modify dest ldap failed: ", $rslt_update_dest->error;
			       } else {
				   die "modify dest ldap failed: ", $rslt_update_dest->error;
			       }
			   }
		       }
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
	       # entry does not exist in dest ldap.  Add them if {dest}{add} is set
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

#		   $entry =~ s/\(/\\(/g;
#		   $entry =~ s/\)/\\)/g;
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
		       if (lc $attr eq "uniquemember") {
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




{
# store timestamps by hostname.  Pull createtimestamp and
# modifytimestamp from each search and store the highest value.  at
# the end of the script run store the value which will be the highest
# create and modify timestamp encountered.  Don't store it sooner,
# otherwise a subsequent search could miss values modified since the
# last run.
    my %timestamps;

    sub check_timestamps {
	my ($_c, $src_struct, $dest, $src_dn, $src_timestamps_struct, $ldap_dest) = @_;

	my %_config = %$_c;
    
	# capture the latest create and modifytimestamp
	my ($work_mod_time, $work_create_time);
    
	my $host = $_config{host};

	if (exists $_config{dest}{$dest}{attrs}) {
	    # attrs is set: we're modifying, use modifytimestamp
	    # TODO: check that modifytimestamp exists in $src_struct->{$src_dn}?
	    $work_mod_time = (@{$src_struct->{$src_dn}->{modifytimestamp}})[0];
	    $work_mod_time =~ s/Z$//;
	    if (!defined $timestamps{lc $host} && 
		!defined $timestamps{lc $host}{modifytimestamp}) {
		$timestamps{lc $host}{modifytimestamp} = $work_mod_time;
		$timestamps{lc $host}{ldap} = $ldap_dest;
		$timestamps{lc $host}{binduser} = $_config{dest}{$dest}{binddn};
	    }
	} else {
	    # we're creating, use createtimestamp
	    if (!defined ($src_timestamps_struct->{$src_dn}->{createtimestamp})) {
		print "no createtimestamp for $src_dn!?\n";

		print Dumper $src_timestamps_struct;
	    } else {
		$work_create_time = (@{$src_timestamps_struct->{$src_dn}->{createtimestamp}})[0];
		$work_create_time =~ s/Z$//;
		if (!defined $timestamps{lc $host} && 
		    !defined $timestamps{lc $host}{createtimestamp}) {
		    $timestamps{lc $host}{createtimestamp} = $work_create_time;
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
    }


    # save the latest timestamps in the description field of the dest sync user
    # this won't work if the user is directory manager of course.
    sub save_timestamps() {
	for my $src_host (keys %timestamps) {
	    my $ct = $timestamps{$src_host}{createtimestamp};
	    my $mt = $timestamps{$src_host}{modifytimestamp};
	    my $ldap = $timestamps{$src_host}{ldap};
	    my $binduser = $timestamps{$src_host}{binduser};

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
		    update_modify(\%modify, "modifytimestamp", $src_host, $mt)
		      if (defined $mt);
		}
	    } else {
		update_modify(\%modify, "createtimestamp", $src_host, $ct)
		  if (defined $ct);
		update_modify(\%modify, "modifytimestamp", $src_host, $mt)
		  if (defined $mt);
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
    }
}

{

    # only return create and modify timestamps once per session
    # otherwise with multiple source host entries the first entry will
    # keep subsequent entries from doing proper searches.

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
    
#    return if (!defined $old_val);

    if (!defined $old_val) {
	# just add
	if (defined $new_val) {
	    if (exists $$mod_ref{add}) {
		push @{$mod_ref->{add}->{description}}, "$type;$src_host;$new_val";
	    } else {
		$$mod_ref{add} = {description => ["$type;$src_host;$new_val"]};
	    }
	}
    } else {
	if (exists $$mod_ref{delete}) {
	    push @{$mod_ref->{delete}->{description}}, "$type;$src_host;$old_val";
	} else {
	    $$mod_ref{delete} = {description => ["$type;$src_host;$old_val"]};
	}

	if (defined $new_val) {
	    if (exists $$mod_ref{add}) {
		push @{$mod_ref->{add}->{description}}, "$type;$src_host;$new_val";
	    } else {
		$$mod_ref{add} = {description => ["$type;$src_host;$new_val"]};
	    }
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

sub get_common_values {
    my $ldap = shift;
    my %c = @_;

    my %to_ignore;

    return () unless (exists $c{ignorecommonvalues});

    for my $v (@{$c{ignorecommonvalues}}) {
	print "\nchecking for common value for ", $v, "...\n"
	  if (exists $opts{d} || exists $opts{a});
	$v = lc $v;
	my $rslt = $ldap->search(base => $c{base}, filter => $c{filter});
	$rslt->code && die "problem searching in get_common_value: ".$rslt->error;

	my %values;

	for my $e ($rslt->entries) {
	    my @value = $e->get_value($v);
	    for my $v (@value) {
		if (exists ($values{$v})) { 
		    $values{$v}++;
		} else {
		    $values{$v} = 1;
		}
	    }
	}

	my ($count, $common_value);
	$count=0;
	for my $k (keys %values) {
	    if ($values{$k} > $count) {
		$common_value = $k;
		$count = $values{$k};
	    }
	}

	#TODO: there's something wrong with this math, I'm seeing a percentage > 100.
	my $percent = ($count / scalar (keys %values)) * 100;
	print "most common $v: $count, $common_value, percentage of total: $percent\n"
	  if (exists $opts{d} || exists $opts{a});

	$to_ignore{$v} = $common_value;
    }

    return %to_ignore;
}



sub get_attr_values (@) {
    my ($_c, $src_struct, $dest_struct, $ldap_src, $ldap_dest, $i, $src_dn, $dest, $dest_dn, 
       $cvti) = @_;
    
    my %_config = %$_c;
    my %common_values_to_ignore = %$cvti;

    my @dest_attrs;
    my @dest_attrs_for_compare;
    my @dest_attrs_to_replace;
    my $l = "";
    my $r = "";
    # keep track of all attrs so they can be saved and re-added later
    # by the caller.  This is designed to handle userpassword on a
    # server with password history turned on.
    my @saved_attrs;
    my @omit_from_l;  # omit these values from $l: at time of writing
                      # they are identified as common values on the
                      # dest but also exist on the src.

    print "working on attr ", $_config{attrs}->[$i], "\n"
      if (exists $opts{a});

    # convert DNs from the src ldap to corresponding DNs in the dest ldap
    if ((lc $_config{attrs}->[$i] =~ /^uniquemember$/) || 
	(lc $_config{attrs}->[$i] =~ /^member$/) ) {
	# go through the src ldap and convert the
	# uniquemembers to DNs in the dest ldap for
	# writing to dest ldap.

	my @src_attrs_for_compare;
	my $updated_rdn = 0;

	for my $member (@{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}) {
	    print "\nconverting source dn: /$member/..\n"
	      if (exists $opts{a});

	    # get the uid of the member attribute
	    my $member_rslt_src = $ldap_src->search(base => $member, 
						    filter => "objectclass=*", attrs => "uid");
	    if ($member_rslt_src->code) {
		if ($member_rslt_src->error eq "No such object") {
		    print "$member (in $src_dn) not in source ldap, removing from dest?\n"
		      if (exists $opts{d});
		    next;
		}

		if ($member_rslt_src->error eq "Invalid DN") {
		    print $member_rslt_src->error, " $member for dn $src_dn\n"
		      if (exists $opts{d} || exists $opts{a});
		    next;
		}
		die "problem searching while converting uniquemember /$member/: " . 
		  $member_rslt_src->error;
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

	    print "searching in dest for uid=$uid in $user_base\n"
	      if (exists $opts{a});

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
	      if (defined $dest_dn && exists $opts{a});

	    push @dest_attrs, $dest_dn if defined $dest_dn;

	    # only populate src_attrs_for_compare if the user is found in the dest ldap.
	    push @src_attrs_for_compare, $dn if defined $dest_dn;
	}

	# go through the members of the dest group
	# and convert them to DNs in the src ldap
	# for comparison.
			       
	for my $member (@{$dest_struct->{$dest_dn}->{$_config{dest}{$dest}{attrs}->[$i]}}) {
	    my $member_rslt_dest = $ldap_dest->search(base => $member, filter => "objectclass=*", 
						      attrs => "uid");
	    if ($member_rslt_dest->code) {
		if ($member_rslt_dest->error eq "No such object") {
		    # $member does not exist in
		    # the source ldap, push it
		    # as-is to the compare
		    # array to cause it to be
		    # cleared from the dest
		    # ldap.
		    push @dest_attrs_for_compare, $member;
		}

		die "problem searching while converting uniquemembers: " . $member_rslt_dest->error
		  unless ($member_rslt_dest->error eq "No such object");
	    } else {
		my $member_struct_dest = $member_rslt_dest->as_struct;
		my $dn = (keys %$member_struct_dest)[0];

		my $uid = @{$member_struct_dest->{$dn}->{uid}}[0];

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
		    #				       next if ($member_rslt_src->error eq "No such object");
		    die "problem searching dest while converting uniquemembers: ". 
		      $member_rslt_src->error
			unless ($member_rslt_src->error eq "No such object");
		}
		my $member_struct_src = $member_rslt_src->as_struct;
		my $src_dn = (keys %$member_struct_src)[0];
		push @dest_attrs_for_compare, $src_dn if defined $src_dn;
	    }
	}

	$l .= " "
	  if ($l !~ "");
	$r .= " "
	  if ($r !~ "");

	$r .= join ' ', sort @dest_attrs_for_compare;
	$l .= join ' ', sort @src_attrs_for_compare;

	# end of uniquemember/member conversion
    } else {

	$l .= " "
	  if ($l !~ "");
	$r .= " "
	  if ($r !~ "");

	# $l .= join ' ', sort @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}
	#   if (defined($src_struct->{$src_dn}->{$_config{attrs}->[$i]}));

	# my @omit_from_l;  # omit these values from $l: at time of
        #                   # writing they are identified as common
        #                   # values on the dest but also exist on the
        #                   # src.
	
	if (defined ($dest_struct->{$dest_dn}->{$_config{dest}{$dest}{attrs}->[$i]})) {
	    for my $v (sort @{$dest_struct->{$dest_dn}->{$_config{dest}{$dest}{attrs}->[$i]}}) {
		if (exists $common_values_to_ignore{$_config{dest}{$dest}{attrs}->[$i]} && 
		    $v eq $common_values_to_ignore{$_config{dest}{$dest}{attrs}->[$i]}) {
		    push @omit_from_l, $v;
		} else {
		    $r .= " "
		      if (($r ne "") && ($r !~ /\s+$/));
		    $r .= $v;
		    push @dest_attrs_to_replace, $v;
		}
		push @saved_attrs, $v;
	    }
	}

	if (defined($src_struct->{$src_dn}->{$_config{attrs}->[$i]})) {
	    for my $v (sort @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}) {
		my $omit=0;
		for my $ov (@omit_from_l) {
		    $omit = 1
		      if ($v eq $ov);
		}
		unless ($omit) {
		    $l .= " "
		      if ($l !~ /^\s*$/);
		    $l .= $v;
		    push @dest_attrs, $v;
		}
	    }
	}

	# @dest_attrs = sort @{$src_struct->{$src_dn}->{$_config{attrs}->[$i]}}
	#   if (defined ($src_struct->{$src_dn}->{$_config{attrs}->[$i]}));
    }

    return ($l, $r, \@dest_attrs, \@dest_attrs_to_replace, \@saved_attrs, \@omit_from_l);
}


sub attr_has_dependencies {
    my ($in_attr, $dependencies) = @_;

    for  my $k (keys %$dependencies) {
	for $a (@{$$dependencies{$k}}) {
	    if (lc $a eq lc $in_attr) {
		return $k, @{$$dependencies{$k}};
	    }
	}
    }
    return 0;
}


sub add_to_modify {
    my ($mods, $attr, $objectclasses, $dest_attrs, $dependency, $dependencies) = @_;

    my $attr_found = 0;
    my $attr_added = 0;
    my $create_new_replace = 1;

    # first check if the attribute is in a replace in @mods
    for my $m (@$mods) {
 	if (exists $m->{replace}) {
 	    for my $a (keys %{$m->{replace}}) {
 		$attr_found = 1
 		  if ($attr eq $a)
 	    }
 	}
    }

    # if $attr is not in mods
    if (!$attr_found) {
	# check to see if an attr in mods is a dependency of $attr and
	# add it to the same replace as the modify will fail otherwise
	for my $m (@$mods) {
	    if (exists $m->{replace}) {
		for my $a (keys %{$m->{replace}}) {
		    $attr_added = 1
		      if (grep (/^$a$/, @$dependencies));
		}

		if ($attr_added) {
		    my %h;
		    for my $a (keys %{$m->{replace}}) {
			$h{$a} = $m->{replace}{$a};
		    }
		    $h{$attr} = $dest_attrs;
		    $m->{replace} = \%h;
		}
	    }
	}
    }

    # if neither the attr or one of its dependencies was found in a modify create a new modify
    if (!$attr_found && !$attr_added) {
	my %modify;
	$modify{replace} = {$attr => $dest_attrs};
	push @$mods, \%modify;
    }

    # if $dependency is not already in the entry
    if (!grep /^$dependency$/i, @$objectclasses) {
	for my $m (@$mods) {
	    # make sure we're replacing an attribute that depends on $dependency
	    if (exists $m->{replace}) {
		my $add_oc = 0;
		for my $a (keys %{$m->{replace}}) {
		    if (grep (/^$a$/, @$dependencies)) {
			if (!grep (/^$dependency$/, keys %{$m->{replace}})) {
			    $add_oc = 1;
			}
		    }
		}

		if ($add_oc) {
		    # if there is already an add check to see if it contains $dependency, add it if not.
		    if (exists $m->{add}) {
			if (exists $m->{add}{objectclass}) {
			    if (!grep /^$dependency$/, @{$m->{add}{objectclass}}) {
				my @l = @{$m->{add}{objectclass}};
				$m->{add}{objectclass} = [@l,$dependency];
			    }
			} else {
			    $m->{add}{objectclass} = $dependency;
			}
		    } else {
			$m->{add} = {"objectclass" => [$dependency]}
		    }
		}
	    }
	}
    }
}
