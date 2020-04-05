package kAligner.classes;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class SequenceKmer {

    private String sequence;
    private Integer position;

    Set<Integer> referencePositions;

    public SequenceKmer(String sequence, Integer position){
        this.sequence = sequence;
        this.position = position;

        referencePositions = new HashSet<>(); 
    }

    public String getSequence(){
        return sequence;
    }

    public Integer getPosition(){
        return position;
    } 

    public Set<Integer> getReferencePositions(){
        return referencePositions;
    }

 

    public void addPositions(ArrayList<Integer> positions){
        referencePositions.addAll(positions);
    }

}