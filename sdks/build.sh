#!/usr/bin/env bash

try_install_buildx() {
    if [[ ! -f ${HOME}/.docker/cli-plugins/docker-buildx ]]; then
        if [[ ! -d "${HOME}/.docker/cli-plugins" ]]; then
            mkdir -p ${HOME}/.docker/cli-plugins/
        else
            echo "Directory '${HOME}/.docker/cli-plugins/' exists"
        fi
        wget -q -O ${HOME}/.docker/cli-plugins/docker-buildx https://github.com/docker/buildx/releases/download/v0.11.1/buildx-v0.11.1.linux-amd64
        chmod +x ${HOME}/.docker/cli-plugins/docker-buildx
    else
        echo "docker-buildx exists"
    fi
}

install_cross() {
    cargo install --list | grep -E 'cross v[0-9]+.[0-9]+.[0-9]+'
    if [ $? == 0 ]; then
        echo "cross has installed"
    else
        cargo install cross
    fi
}

build_shared_libraries() {
    # Install cross
    install_cross
    # sudo chown -R $USER:$USER .
    cross build -p frontend --target aarch64-unknown-linux-gnu -v | exit 1
    cross build -p frontend --target x86_64-unknown-linux-gnu -v | exit 1
    cp target/aarch64-unknown-linux-gnu/debug/libfrontend.so sdks/frontend-java/client/src/main/resources/META-INF/native/libfrontend_linux_aarch_64.so | exit 1
    cp target/x86_64-unknown-linux-gnu/debug/libfrontend.so sdks/frontend-java/client/src/main/resources/META-INF/native/libfrontend_linux_x86_64.so | exit 1
}

BASEDIR=$(dirname "$0")
cd "$BASEDIR/.." || exit

try_install_buildx

build_shared_libraries

cd sdks/frontend-java || exit
mvn -DargLine="--add-opens=java.base/java.nio=ALL-UNNAMED" package
