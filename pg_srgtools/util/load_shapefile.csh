##!/bin/csh -fx

# Loading the data shapefile first and then reproject to the output modeling domain
# Loading once for processing multiple modeling domains

# 1. Load the original data shapefile (shp) with its projection (prj)

setenv shp_tbl `echo $shapefile | tr "[:upper:]" "[:lower:]"`

echo $shapefile
#if ( $shp_tbl == "acs_2014_5yr_pophousing" ) then
$GDALBIN/ogr2ogr -f "PostgreSQL" "PG:dbname=$dbname user=$user host=$server" $indir/$shapefile.shp -lco PRECISION=NO -nlt PROMOTE_TO_MULTI -nln $schema.$table -overwrite
echo "Finished loading shapefile: " $indir/$shapefile.shp

# 2. Tranform to a new projection (from original to new 900921/900915) and create gist index on it
$PGBIN/psql -h $server -U $user -q $dbname << END1
ALTER TABLE $schema.$table ADD COLUMN $newfield geometry($geomtype, $srid);
-- CREATE INDEX ON $schema.$table USING GIST ($newfield);
DROP INDEX if exists $schema.${table}_${org_geom_field}_geom_idx;
DROP INDEX if exists $schema.${table}_${newfield}_idx;
UPDATE $schema.$table SET $newfield = ST_Transform($org_geom_field, $srid);
CREATE INDEX on $schema.${table} using GIST(geom_$srid );
END1

# 3. Check whether the shapefile data are imported correclty or not.
$PGBIN/psql -h $server -U $user -q $dbname << END1
update ${schema}.${table}
        SET ${newfield} = ST_MakeValid(${newfield})
       WHERE NOT ST_IsValid(${newfield});
END1
#UPDATE $schema.$table SET $newfield = ST_Transform($org_geom_field, $srid);
#UPDATE $schema.$table SET $newfield = ST_Buffer($newfield, 0.0);
#DROP INDEX if exists $schema.${table}_${org_geom_field}_geom_idx;

# Create ntad_2017_county_pol based on 2014
if ( $table == "ntad_2014_county_pol" ) then
$PGBIN/psql -h $server -U $user -q $dbname << END1
  DROP TABLE  ntad_2017_county_pol;
  CREATE TABLE ntad_2017_county_pol as select * FROM ntad_2014_county_pol;
  UPDATE ntad_2017_county_pol set ctfips='46102' WHERE  ctfips='46113';
  UPDATE $schema.$table SET $newfield = ST_Transform($org_geom_field, $srid);
  UPDATE $schema.$table SET $newfield = ST_Buffer($newfield, 0.0);
  DROP INDEX if exists $schema.${table}_${org_geom_field}_geom_idx;
  DROP INDEX if exists $schema.${table}_${newfield}_idx;
  CREATE INDEX on $schema.${table} using GIST(geom_$srid );
END1
endif

# add columns
#if ( $table == "hpms2016" ||  $table == "hpms2017_update" ) then
if ( $table == "hpms2016" ||  $table == "hpms2017_v3_04052020" ) then
$PGBIN/psql -h $server -U $user -q $dbname << END1
  ALTER TABLE $schema.$table ALTER COLUMN moves2014 TYPE INT USING moves2014::integer;
  ALTER TABLE $schema.$table
    add column length_$srid double precision,
    add column aadt_dens_${srid} double precision;
  update $schema.$table
    set length_${srid} = ST_Length(geom_${srid});
  update $schema.$table
    set aadt_dens_${srid} = aadt/length_${srid};
END1
endif


# add column geoid (stctyfips) to "ntad_2014_rail" table
if ( $table == "ntad_2014_rail" ) then
$PGBIN/psql -h $server -U $user -q $dbname << END1
  ALTER TABLE $schema.$table add column geoid varchar(5);
  update $schema.$table set geoid=concat(statefips, cntyfips);
END1
endif


#
#if ( $table == "ntad_2014_ipcd" ) then
#psql  -h $server -U $user -q $dbname << END1
#  ALTER TABLE $schema.$table RENAME pt_id_lon TO longitude;
#  ALTER TABLE $schema.$table RENAME pt_id_lat TO latitude;
#END1
#endif

#if ( $table == "poi_factory_2015_golfcourses" ) then
#psql  -h $server -U $user -q $dbname << END1
#  ALTER TABLE $schema.$table RENAME lon TO longitude;
#  ALTER TABLE $schema.$table RENAME lat TO latitude;
#END1
#endif
#if ( $table == "ertac_railyard_wrf" ) then
#psql -h $server -U $user -q $dbname << END1
#  ALTER TABLE $schema.$table RENAME lon TO longitude;
#  ALTER TABLE $schema.$table RENAME lat TO latitude;
#END1
#endif


# Calculate density
if ( $table == "acs_2014_5yr_pophousing" ) then
$PGBIN/psql -h $server -U $user -q $dbname << END1
  ALTER TABLE $schema.$table
        add column area_$srid double precision,
        add column pop2014_dens_${srid} double precision,
        add column hu2014_dens_${srid} double precision,
        add column popch14_10_dens_${srid} double precision,
        add column huch14_10_dens_${srid} double precision,
        add column pop2010_dens_${srid} double precision,
        add column hu2010_dens_${srid} double precision,
        add column pop2000_dens_${srid} double precision,
        add column hu2000_dens_${srid} double precision,
        add column util_gas_dens_${srid} double precision,
        add column wood_dens_${srid} double precision,
        add column fuel_oil_dens_${srid} double precision,
        add column coal_dens_${srid} double precision,
        add column lp_gas_dens_${srid} double precision,
        add column elec_dens_${srid} double precision,
        add column solar_dens_${srid} double precision;
  update $schema.$table
        set area_$srid =ST_Area(geom_$srid );
  update $schema.$table set
        pop2014_dens_${srid}=pop2014 / area_$srid ,
        hu2014_dens_${srid}=hu2014 / area_$srid ,
        popch14_10_dens_${srid}=popch14_10 / area_9$srid 
        huch14_10_dens_${srid}=huch14_10 / area_$srid ,
        pop2010_dens_${srid}=pop2010 / area_$srid ,
        hu2010_dens_${srid}=hu2010 / area_$srid ,
        pop2000_dens_${srid}=pop2000 / area_$srid ,
        hu2000_dens_${srid}=hu2000 / area_$srid ,
        util_gas_dens_${srid}=util_gas / area_$srid ,
        wood_dens_${srid}=wood / area_$srid ,
        fuel_oil_dens_${srid}=fuel_oil / area_$srid ,
        coal_dens_${srid}=coal / area_$srid ,
        lp_gas_dens_${srid}=lp_gas / area_$srid ,
        elec_dens_${srid}=elec / area_$srid ,
        solar_dens_${srid}=solar / area_$srid ;
END1
endif

if ( $table == "acs2016_5yr_bg" ) then
$PGBIN/psql -h $server -U $user -q $dbname << END1
  ALTER TABLE $schema.$table
        add column area_$srid  double precision,
        add column pop2016_dens_${srid} double precision,
        add column hu2016_dens_${srid} double precision,
        add column util_gas_dens_${srid} double precision,
        add column wood_dens_${srid} double precision,
        add column fuel_oil_dens_${srid} double precision,
        add column coal_dens_${srid} double precision,
        add column lp_gas_dens_${srid} double precision,
        add column elec_dens_${srid} double precision,
        add column solar_dens_${srid} double precision;
  update $schema.$table
        set area_$srid =ST_Area(geom_$srid );
  update $schema.$table set
        pop2016_dens_${srid}=pop2016 / area_$srid ,
        hu2016_dens_${srid}=hu2016 / area_$srid ,
        util_gas_dens_${srid}=util_gas / area_$srid ,
        wood_dens_${srid}=wood / area_$srid ,
        fuel_oil_dens_${srid}=fuel_oil / area_$srid ,
        coal_dens_${srid}=coal / area_$srid ,
        lp_gas_dens_${srid}=lp_gas / area_$srid ,
        elec_dens_${srid}=elec / area_$srid ,
        solar_dens_${srid}=solar / area_$srid ;
END1
endif


if ( $table == "shippinglanes_2014nei" || $table == "ports_2014nei" ) then
psql -h $server -U $user -q $dbname << END1
  ALTER TABLE $schema.$table
        add column area_$srid  double precision,
        add column area_sqmi_dens_${srid} double precision,
        add column activitykw_dens_${srid} double precision;
  update $schema.$table
        set area_$srid =ST_Area(geom_$srid );
  update $schema.$table set
        area_sqmi_dens_${srid}=area_sqmi/area_$srid ,
        activitykw_dens_${srid}=activitykw/area_$srid ;
END1
endif

if ( $table == "fema_bsf_2002bnd" ) then
psql -h $server -U $user -q $dbname << END1
  ALTER TABLE ${schema}.${table}
        add column su_500 double precision,
        add column su_505 double precision,
        add column su_506 double precision,
        add column su_510 double precision,
        add column su_535 double precision;
   update ${schema}.${table} set
       su_500=com1+com2+com3+com4+com5+com6+com7+com8+com9,
       su_505=ind1+ind2+ind3+ind4+ind5+ind6,
       su_506=edu1+edu2,
       su_510=com1+com2+com3+com4+com5+com6+com7+com8+com9+ind1+ind2+ind3+ind4+ind5+ind6,
       su_535=com1+com2+com3+com4+com5+com6+com7+com8+com9+ind1+ind2+ind3+ind4+ind5+ind6+edu1+edu2+rel1+gov1+gov2+res1+res2+res3+res4;
  ALTER TABLE ${schema}.${table}
        add column area_$srid  double precision;
  update ${schema}.${table}
        set area_$srid =ST_Area(geom_$srid );
  ALTER TABLE ${schema}.${table}
        add column su_500_dens_$srid double precision,
        add column su_505_dens_$srid double precision,
        add column su_506_dens_$srid double precision,
        add column su_510_dens_$srid double precision,
        add column su_535_dens_$srid double precision,
        add column com6_dens_$srid double precision;
  update ${schema}.${table} set
        su_500_dens_$srid=su_500 / area_$srid ,
        su_505_dens_$srid=su_505 / area_$srid ,
        su_506_dens_$srid=su_506 / area_$srid,
        su_510_dens_$srid=su_510 / area_$srid ,
        su_535_dens_$srid=su_535 / area_$srid ,
        com6_dens_$srid=com6 / area_$srid ;
END1
endif

# polygons, oil gas
echo $attr   $geomtype
if ( $attr != "" && $geomtype == "MultiPolygon" ) then
psql -h $server -U $user -q $dbname << END1
  ALTER TABLE ${schema}.${table}
        add column ${attr}_dens_${srid} double precision,
        add column area_$srid  double precision;
  update ${schema}.$table
        set area_$srid =ST_Area(geom_$srid );
  update ${schema}.$table set
        ${attr}_dens_${srid}=${attr}/area_$srid ;
END1
endif



