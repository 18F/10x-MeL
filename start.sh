docker_state=$(docker info >/dev/null 2>&1)

if [[ $? -ne 0 ]]; then
    echo 
    echo Docker does not seem to be running. Please launch Docker and then try again.
    echo
    exit 1
fi

docker-compose up --build -d

echo Please open the following URL in your browser:
echo
echo http://localhost:5000/
echo