FROM mcr.microsoft.com/devcontainers/cpp:ubuntu-22.04

WORKDIR /opt
COPY ../third-party/xnvme/toolbox ./toolbox
RUN bash -e ./toolbox/pkgs/ubuntu-jammy.sh

RUN rm -rf /usr/local/vcpkg

CMD ["bash"]