from ghcr.io/xnvme/xnvme-deps-alpine-latest:next

RUN apk update && apk --no-cache add gpg gnupg
RUN echo "export GPG_TTY=$(tty)" >> ~/.bashrc

CMD ["bash"]