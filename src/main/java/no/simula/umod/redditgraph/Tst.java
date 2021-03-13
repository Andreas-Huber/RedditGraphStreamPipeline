package no.simula.umod.redditgraph;

import com.opencsv.bean.CsvBindByName;


public class Tst {


    @CsvBindByName(column = "i", required = true)
    private String subreddit;
}