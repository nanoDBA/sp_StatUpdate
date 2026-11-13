FROM mcr.microsoft.com/mssql/server:2022-latest

COPY . /

USER root

RUN chmod +x db-init.sh entrypoint.sh

# Enable SQL Agent and HADR
RUN /opt/mssql/bin/mssql-conf set sqlagent.enabled true
RUN /opt/mssql/bin/mssql-conf set hadr.hadrenabled 1
RUN /opt/mssql/bin/mssql-conf set memory.memorylimitmb 2048

CMD /bin/bash ./entrypoint.sh
