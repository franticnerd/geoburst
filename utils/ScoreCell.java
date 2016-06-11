package utils;

/**
 * A class that store the score of a candidate event
 */
public class ScoreCell {
    int id;
    double score;
    public ScoreCell(int id, double score) {
        this.id = id;
        this.score = score;
    }
    public int getId() {
        return id;
    }
    public double getScore() {
        return score;
    }

    @Override
    public String toString() {
        return "[" + id + "," + score + "]";
    }

    @Override
    public int hashCode() {
        return new Integer(id).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if(!(obj instanceof ScoreCell))
            return false;
        ScoreCell sc = (ScoreCell) obj;
        if (sc.getId() == this.id)
            return true;
        else
            return false;
    }

}
