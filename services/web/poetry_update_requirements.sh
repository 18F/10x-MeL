timestamp=$( date "+%s" )
req="requirements.txt"

cp ${req} ${req}.${timestamp}

poetry export --without-hashes -f requirements.txt > ${req}

