#!/bin/bash

set -oe

# All credit for this file goes to @mandreyel, creator of cratetorrent.
# Excellent test setup that I've stolen.

# Returns the container's IP address in the local Docker network.
#
# Arguments:
# - $1 container name
function get_container_ip {
    cont=$1
    podman inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "${cont}"
}

function print_help {
    echo -e "
This script starts a Transmission seed container with the specified parameters,
if it's not already running.
USAGE: $1 --name NAME
OPTIONS:
    -n|--name   The name to give to the container.
    -h|--help   Print this help message.
    "
}

for arg in "$@"; do
    case "${arg}" in
        -n|--name)
            name=$2
        ;;
        -p|--path)
            path=$2
        ;;
        -t|--torrent-name)
            torrent_name=$2
        ;;
        --h|--help)
            print_help
            exit 0
        ;;
    esac
    shift
done

if [ -z "${name}" ]; then
    echo "Error: --name must be set"
    print_help
    exit 1
fi

if [ -z "${torrent_name}" ]; then
    echo "Error: --torrent-name must be set"
    print_help
    exit 1
fi

if [ ! -f "${path}" ] && [ ! -d "${path}" ]; then
    echo "Error: torrent file does not exist at ${path}"
    exit 6
fi

# where the torrent file and metainfo are saved
assets_dir="$(pwd)/assets"
if [ -f "${assets_dir}" ]; then
    echo "Error: file found at assets directory path ${assets_dir} "
    exit 2
elif [ ! -d "${assets_dir}" ]; then
    echo "Creating assets directory ${assets_dir}"
    mkdir "${assets_dir}"
fi

# initialize the directories of the seed, if needed
#
# NOTE: the paths given are on the host, not inside the container
tr_seed_dir="${assets_dir}/${name}"
if [ ! -d "${tr_seed_dir}" ]; then
    echo "Creating seed ${name} directory at ${tr_seed_dir}"
    mkdir "${tr_seed_dir}"
fi
tr_config_dir="${tr_seed_dir}/config"
tr_downloads_dir="${tr_seed_dir}/downloads"
tr_watch_dir="${tr_seed_dir}/watch"
# create the subdirectories that we're binding into the container (must exist
# before bind mounting)
for subdir in {"${tr_config_dir}","${tr_downloads_dir}","${tr_watch_dir}"}; do
    if [ ! -d "${subdir}" ]; then
        echo "Creating seed subdirectory ${subdir}"
        mkdir "${subdir}"
    fi
done

torrent_path="${tr_downloads_dir}/complete/${torrent_name}"
if [ "${path}" != "${torrent_path}" ]; then
    echo "Copying torrent from source ${path} to seed dir at ${torrent_path}"
    mkdir -p "${tr_downloads_dir}/complete"
    cp -r "${path}" "${torrent_path}"
fi

# check if the seed is running: if not, start it
if [[ "$(podman inspect --format '{{.State.Running}}' "${name}" 2>/dev/null)" != "true" ]];
then
    echo "Starting Transmission seed container ${name} listening on port 51413"
    podman run \
        --rm \
        --name "${name}" \
        --env PUID=$UID \
        --env PGID=$UID \
        --replace \
        -v "${tr_config_dir}:/config:Z" \
        -v "${tr_downloads_dir}:/downloads:Z" \
        -v "${tr_watch_dir}:/watch:Z" \
        -p 51413:51413 \
        --detach \
        linuxserver/transmission

    seed_ip="$(get_container_ip "${name}")"
    echo "Seed available on local net at IP: ${seed_ip}"

    # wait for seed to come online
    sleep 5

    echo "Transmission seed ${name} started!"
else
    echo "Transmission seed ${name} already running!"
fi

tr_metainfo_basepath="${tr_watch_dir}/${torrent_name}"
tr_metainfo_path="${tr_metainfo_basepath}.torrent"
assets_metainfo_path="${assets_dir}/${torrent_name}.torrent"

# The metainfo for the torrent may already exist if another seed is already
# seeding it. If it doesn't, create it inside the seed container and
# copy the metainfo into the assets folder so that it is available for others.
# If it exists, we just need to copy the existing metainfo inside the
# container.
if [ -f "${assets_metainfo_path}" ]; then
    # the metainfo in assets is just a symlink so we need to follow it
    sudo cp --dereference "${assets_metainfo_path}" "${tr_metainfo_path}"

    # wait for Transmission to pick up the file
    sleep 5
else 
    # NOTE: The file is not created with the `.torrent` suffix on purpose! Since the
    # Transmission container can only be run as root, the metainfo file is also
    # created as root.  However, this would cause a permission denied error for the
    # Transmission container itself, as it is running as the specified user/group.
    # By not specifying the `.torrent` suffix, Transmission won't pick it up, so we
    # get a chance to change its permissions before adding the suffix. 
    #
    # It may be possible to solve this by spawning a subshell with a different EUID
    # and execute the `transmission-create` command there, but currently it is not
    # clear how to do this.
    echo "Creating torrent metainfo file"
    podman exec "${name}" transmission-create \
      -o "/watch/${torrent_name}" \
      "/downloads/complete/${torrent_name}"
    # change ownership of the metainfo file to the same user whose `UID` and `GID`
    # were given to the seed container
    #
    # TODO: make this work without sudo
    echo "Changing metainfo owner from root to $USER"
    podman unshare chown $USER:$USER "${tr_metainfo_basepath}"
    # rename the torrent file to have the `.torrent` suffix, which will make the
    # Transmission daemon automatically start seeding the torrent
    echo "Adding .torrent suffix to metainfo filename"
    sudo mv "${tr_metainfo_basepath}" "${tr_metainfo_path}"

    # we need to add the `.added` suffix to our path as that's what Transmission does after
    # picking up a new metainfo file
    tr_metainfo_path="${tr_metainfo_path}.added"
    sudo chmod o+r "${tr_metainfo_path}"
    # sanity check
    if [ ! -f "${tr_metainfo_path}" ]; then
        echo "Error: could not find metainfo ${tr_metainfo_path} after starting torrent"
        exit 7
    fi
    # link metainfo file in the transmission watch directory to the root of the
    # assets dir for use by other scrips
    echo "Linking metainfo to assets directory root"
    ln -s "${tr_metainfo_path}" ${assets_metainfo_path}
fi

echo "Done!"
echo "Torrent ${torrent_name} is seeded by container ${seed_container}"
