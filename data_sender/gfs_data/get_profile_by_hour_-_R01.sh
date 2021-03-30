#!/bin/sh 
 
export PATH=$PATH:$HOME/bin
export WORK="/home/sodarec/data_sender/gfs_data/"

cd ${WORK}

mesext=`date +"%b" | tr "[:lower:]" "[:upper:]"`
data=`date +"%Y%m%d"`
hh=`date +"0%H"`

anoyy=`echo ${data} | cut -c3-4`
ano=`echo ${data} | cut -c1-4`
mes=`echo ${data} | cut -c5-6`
dia=`echo ${data} | cut -c7-8`
/bin/rm -vf profile_${data}${hh}.dat 
/bin/rm -vf sodar_${data}${hh}.dat 
/bin/rm -vf wget-log* 


/bin/rm -v gfs.t00z.pgrb2.0p25.f${hh} 
echo
gfsinfo="gfs.${data}/00/gfs.t00z.pgrb2.0p25.f${hh}"
wget ftp://ftp.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/$gfsinfo 
echo


if [ -s gfs.t00z.pgrb2.0p25.f${hh} ] 
then
  echo
  echo "GFS FILE FORECAST "${hh}" TIME GREATER THAN ZERO, LET'S GO!"
  echo "-----------------------------------------------------------"

  echo "Extracting souding profile from 1000 to 300 mmHg"
  /bin/rm -vf profile_${data}${hh}.tmp 
  for npres in `echo 1000 975 950 925 900 850 800 750 700 650 600 550 500 450 400 350 300`
  do
     altq=`wgrib2 gfs.t00z.pgrb2.0p25.f${hh} | grep HGT | egrep "(:${npres} mb:)" | \
           wgrib2 -i gfs.t00z.pgrb2.0p25.f${hh} -s -ijlat 1266 269 | cut -d= -f5`

     temp=`wgrib2 gfs.t00z.pgrb2.0p25.f${hh} | grep TMP | egrep "(:${npres} mb:)" | \
           wgrib2 -i gfs.t00z.pgrb2.0p25.f${hh} -s -ijlat 1266 269 | cut -d= -f5`

     dewpt="32767"

     ugrd=`wgrib2 gfs.t00z.pgrb2.0p25.f${hh} | grep UGRD| egrep "(:${npres} mb)"  | \
           wgrib2 -i gfs.t00z.pgrb2.0p25.f${hh} -s -ijlat 1266 269 | cut -d= -f5`

     vgrd=`wgrib2 gfs.t00z.pgrb2.0p25.f${hh} | grep VGRD| egrep "(:${npres} mb)"  | \
           wgrib2 -i gfs.t00z.pgrb2.0p25.f${hh} -s -ijlat 1266 269 | cut -d= -f5`

     echo ${ugrd}" "${vgrd} > uv.asc
     wdir=`./atan2f  | awk '{printf $1"\n"}'`
     speed=`./atan2f | awk '{printf $2"\n"}'`

     echo "  4 "$npres" "$altq" "$temp" "$dewpt" "$wdir" "$speed >> profile_${data}${hh}.tmp 

  done

#   File header includes NLEVS, so the header is the final step.
  nlevs=`wc -l profile_${data}${hh}.tmp | awk '{printf $1"\n"}'` 
  nlevs=`expr ${nlevs} \+ 4`
  echo "254    "${hh}"     "${dia}"   "${mesext}"   "${ano}" "  >> profile_${data}${hh}.dat
  echo "  1  89999  83746  22.90  43.74     00  32767"          >> profile_${data}${hh}.dat
  echo "  2  32767  32767  32767     ${nlevs}  32767  32767"    >> profile_${data}${hh}.dat
  echo "  3           TER                          ms"          >> profile_${data}${hh}.dat

  cat  profile_${data}${hh}.tmp                                      >> profile_${data}${hh}.dat
    
  echo "Extracting sodar like profile from 80 to 5000 meters"
  /bin/rm -vf sodar_${data}${hh}.tmp 
#   for hgt in `echo 80 100 1829 2743 3658`
  for hgt in `echo 80 100 1829`
  do
     temp=`wgrib2 gfs.t00z.pgrb2.0p25.f${hh} | egrep "(:TMP:${hgt} m above)"   | \
           wgrib2 -i gfs.t00z.pgrb2.0p25.f${hh} -s -ijlat 1266 269             | \
           cut -d= -f5`

     ugrd=`wgrib2 gfs.t00z.pgrb2.0p25.f${hh} | egrep "(:UGRD:${hgt} m above)"  | \
           wgrib2 -i gfs.t00z.pgrb2.0p25.f${hh} -s -ijlat 1266 269             | \
           cut -d= -f5`

     vgrd=`wgrib2 gfs.t00z.pgrb2.0p25.f${hh} | egrep "(:VGRD:${hgt} m above)"  | \
           wgrib2 -i gfs.t00z.pgrb2.0p25.f${hh} -s -ijlat 1266 269             | \
           cut -d= -f5`

     echo ${ugrd}" "${vgrd} > uv.asc
     wdir=`./atan2f  | awk '{printf $1"\n"}'`
     speed=`./atan2f | awk '{printf $2"\n"}'`

     echo
     echo ${dia}" "${mes}" "${anoyy}" "${hh}" 00 "${hgt}" 99 99 "${temp}" "${wdir}" "${speed} >> sodar_${data}${hh}.tmp

# OSDY OSMO OSYT OSHR OSMN HT01 SA01 SW01 TT01 WD01 WS01
#                          HT02 SA02 SW02 TT02 WD02 WS02
#                          HT03 SA03 SW03 TT03 WD03 WS03
#
# HT - Height
# SA - Std Dev horizontal wind
# SW - Std Dev w-comp of wind
# TT - Temperature
# WD - Wind direction
# WS - Wind speed
#
# 1  1 88  1  0
# 1  1 88  1  0   10.0  57.2   0.020   0.64  286.00    0.50
# 1  1 88  1  0   50.0  31.3 -99.000   1.34   35.00    0.60
# 1  1 88  1  0  100.0  46.4   0.140   1.34  146.00    1.30
# 1  1 88  2  0
# 1  1 88  2  0   10.0  40.0   0.020   1.04  253.00    0.60
# 1  1 88  2  0   50.0  35.6 -99.000   1.64  168.00    0.80
# 1  1 88  2  0  100.0  10.8   0.210   1.74  172.00    2.10

  done

#   File header is the date-time frame line 
  echo ${dia}" "${mes}" "${anoyy}" "${hh}" 00 " >> sodar_${data}${hh}.dat
  cat sodar_${data}${hh}.tmp                         >> sodar_${data}${hh}.dat 

else
  echo
  echo "GFS FILE FORECAST "${hh}" TIME NOT FOUND !"
  echo
fi

python3 gfs_data_sender.py

# if python3 gfs_data_sender.py 2>&1 >/dev/null; then
#     echo 'script ran fine'
# ${exit_status}
# fi


exit_status=$?
if [ "${exit_status}" -ne 0 ]; then
    mv profile_${data}${hh}.dat /home/sodarec/data_sender/gfs_data/inseridos/
    /bin/rm -vf sodar_${data}${hh}.dat
    /bin/rm -vf uv.asc
    /bin/rm -vf profile_${data}${hh}.tmp 
    /bin/rm -vf sodar_${data}${hh}.tmp 
    /bin/rm -vf gfs.t00z.pgrb2.0p25.f${hh}
else
    mv profile_${data}${hh}.dat /home/sodarec/data_sender/gfs_data/nao_inseridos/ 
    /bin/rm -vf sodar_${data}${hh}.dat 
    /bin/rm -vf uv.asc
    /bin/rm -vf profile_${data}${hh}.tmp 
    /bin/rm -vf sodar_${data}${hh}.tmp 
    /bin/rm -vf gfs.t00z.pgrb2.0p25.f${hh}
fi


#/bin/rm -vf profile_${data}${hh}.dat 
#/bin/rm -vf sodar_${data}${hh}.dat 
#/bin/rm -vf uv.asc
#/bin/rm -vf profile_${data}${hh}.tmp 
#/bin/rm -vf sodar_${data}${hh}.tmp 
#/bin/rm -vf gfs.t00z.pgrb2.0p25.f${hh}


# FSL Format Resource
# https://ruc.noaa.gov/raobs/fsl_format-new.html#:~:text=FSL%20Output%20Format%20Description&text=The%20first%204%20lines%20of,not%20reported%2C%20or%20not%20applicable

# FORTRAN FORMAT 
#                      254    (3i7,6x,a4,i7)
#                        1    (3i7,f7.2,a1,f6.2,a1,i6,i7)
#                        2    (7i7)
#                        3    (i7,10x,a4,14x,i7,5x,a2)
#              4,5,6,7,8,9    (7i7) 

# MEMORY
#
# Ternium Site Longitude
# 43d 44m 12.75s
# (((12.75/60)+44)/60)+43 = 43.73687500000000000000 degrees
# World Coordinates: 360-43.736875 = 316.263125
# Point i=1266 equal to longitude 316.250000
#
# Ternium Site Latitude
# 22d 54m 27.40s
# (((27.40/60)+54)/60)+22 = 22.90761111111111111111 degrees 
# World Coordinates: -22.90761 
# Point j=269 equal to latitude -23.0000
#
# WGRIB2 Command:
# wgrib2 gfs.t00z.pgrb2.0p25.f${hh} | grep HGT | egrep "(:${npres} mb:)" | \
#            wgrib2 -i gfs.t00z.pgrb2.0p25.f${hh} -s -ijlat 1266 269 | cut -d= -f5`
#

