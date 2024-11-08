# pod-traffic-tickets

# Trabajo Práctico Especial 2: Multas de Estacionamiento

Programacion de objetos distribuidos

## Grupo 6

- Rossi, Nicolas
- Ricarte, Matias Agustin
- Panighini, Franco

## Instrucciones

### Build

1. `cd` al root del proyecto
2. Ejecutar el comando `mvn clean install`.
3. Descomprimir los builds del cliente y el servidor con los comandos: tar -xzf tpe2-g6-parent6/client/target/tpe2-g6-client-2024.2Q-bin.tar.gz y tar -xzf tpe2-g6-parent/server/target/tpe2-g6-server-2024.2Q-bin.tar.gz, esto creara las carpetas `tpe2-g6-parent-client-2024.2Q` y `tpe2-g6-parent-server-2024.2Q`

### Iniciar el servidor

1. `cd` a `tpe2-g6-parent-server-2024.2Q`.
2. Ejecutar el comando `chmod +x ./run-server.sh`
3. Correr el servidor usando en un puerto determinado `./run-server.sh`

### Usar el cliente

1. `cd` a `tpe2-g6-parent-client-2024.2Q`.
2. Dar permisos de ejecucion a los scripts `query1.sh`, `query2.sh`, `query3.sh`, `query4.sh`

### Ejemplos de uso

Parametros:

- `addresses`: Direcciones de IP y puertos de los nodos del servidor
- `city`: Ciudad elegida, puede ser Chicago `CHI`, o New York `NYC`
- `inPath`: path donde están los archivos de entrada de multas, infracciones y agencias.
- `outPath`: path donde estarán ambos archivos de salida `queryX.csv` y `timeX.txt`.
- `readerType`: Tipo de reader a usar, por default usa `parallel`

**Query 1** con reader secuencial

```bash
./query1 -Daddresses='10.6.0.1:5701;10.6.0.2:5701'
          -Dcity=NYC
          -DinPath=/afs/it.itba.edu.ar/pub/pod/
          -DoutPath=/afs/it.itba.edu.ar/pub/pod-write/
          -DreaderType=sequential
```

**Query 1** con reader paralelo

```bash
./query1 -Daddresses='10.6.0.1:5701;10.6.0.2:5701'
          -Dcity=NYC
          -DinPath=/afs/it.itba.edu.ar/pub/pod/
          -DoutPath=/afs/it.itba.edu.ar/pub/pod-write/
```

**\*Query 1** con fastCSV reader

```bash
./query1 -Daddresses='10.6.0.1:5701;10.6.0.2:5701'
          -Dcity=NYC
          -DinPath=/afs/it.itba.edu.ar/pub/pod/
          -DoutPath=/afs/it.itba.edu.ar/pub/pod-write/
          -DreaderType=fastcsv
```

**Query 2** con reader secuencial

```bash
./query2.sh -Daddresses='10.6.0.1:5701' -DreaderType=sequential -Dcity=NYC  -DinPath=. -DoutPath=.
```

**Query 3** con reader paralelo

```bash
./query3.sh -Daddresses='10.6.0.1:5701' -Dcity=NYC
          -DinPath=. -DoutPath=.
          -Dn=2
          -Dfrom=01/01/2021 -Dto=31/12/2021
```

**Query 4** con reader secuencial

```bash
./query4.sh -Daddresses='10.6.0.1:5701'
            -Dcity=CHI
            -DinPath=. -DoutPath=.
            -Dn=3
            -Dagency=CPD-Airport
            -DreaderType=sequential
```
