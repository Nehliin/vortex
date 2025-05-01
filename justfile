default:
    just --list

container-name := "transmission-1"

@generate-test-file name="test-file-1" size="100000000":
    mkdir -p assets/
    truncate -s "{{size}}" "assets/{{name}}"
    head -c "{{size}}" < /dev/urandom > "assets/{{name}}"
    chmod +r "assets/{{name}}"
    echo "File randomly generated at {{name}} with {{size}} bytes"
    
setup-transmission file-name="test-file-1":
    @echo {{ if path_exists(prepend("assets/", file-name)) == "true" { "File exists" } else { shell("just generate-test-file $1", file-name) } }}
    ./scripts/transmission_containers.sh --name {{container-name}} --path assets/{{file-name}} --torrent-name {{file-name}}

test: setup-transmission
    cargo nextest run --workspace 

clean:
    sudo rm -rf assets/
    -podman kill {{container-name}}
    cargo clean


setup-grafana:
  podman-compose up --force-recreate -d


test-locally: setup-transmission setup-grafana
    cargo test --test basic

