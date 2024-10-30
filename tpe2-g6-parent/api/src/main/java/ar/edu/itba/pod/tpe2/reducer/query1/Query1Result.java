package ar.edu.itba.pod.tpe2.reducer.query1;

public record Query1Result(String infraction, String agency, long tickets) {
    @Override
    public String toString() {
        return "%s;%s;%d".formatted(infraction, agency, tickets);
    }
}
