echo $1
python3.9 /var/www/awaire-air-quality-aermod/aermod_input_creator/aermod_input_creator_r01.py '$1' > input_creator_log.txt
./aermet.exe s1.inp
./aermet.exe s2.inp
./aermet.exe s3.inp
./aermod.exe
