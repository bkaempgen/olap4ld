package org.olap4j.driver.ld;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class HumaniseCamelCase {
    //----------------------------------------------------------------------- Static Properties and Constants

    private static final String CAMEL_CASE_PATTERN = "([A-Z]|[a-z])[a-z]*";
    
    private static final String CAMEL_CASE_PATTERN_NUMBERS = "([0-9]*[-]*)*";

    //----------------------------------------------------------------------- Instance Properties

    private String humanisedString;
    private String acronym;

    //----------------------------------------------------------------------- Instance Methods

    /**
     * Converts a camelCase to a more human form, with spaces. E.g. 'Camel case'
     *
     * TODO: Takes also numbers, adds them at the end, only.
     *
     * @param camelCaseString
     * @return a humanised version of a camelCaseString if it is indeed camel-case. Returns the
     * original string if it is'nt camel-case
     */
    public String humanise(String camelCaseString) {
        reset();
        Matcher wordMatcher = camelCaseWordMatcher(camelCaseString);
        while(wordMatcher.find()) {
            String word = wordMatcher.group();
            boolean wordIsSingleCapitalLetter = word.matches("^[A-Z]$");
            if(wordIsSingleCapitalLetter) {
                addToAcronym(word);
            } else {
                appendAcronymIfThereIsOne();
                appendWord(word);
            }
        }
        humanisedString += " ";
        Matcher numberMatcher = camelCaseNumberMatcher(camelCaseString);
        while(numberMatcher.find()) {
            String number = numberMatcher.group();
            boolean wordIsSingleCapitalLetter = number.matches("^[0-9]$");
            if(wordIsSingleCapitalLetter) {
                addToAcronym(number);
            } else {
                appendNumber(number);
            }
        }
        appendAcronymIfThereIsOne();
        return humanisedString.length() > 0 ? humanisedString : camelCaseString;
    }

    private Matcher camelCaseWordMatcher(String camelCaseString) {
        return Pattern.compile(CAMEL_CASE_PATTERN).matcher(camelCaseString);
    }
    
    private Matcher camelCaseNumberMatcher(String camelCaseString) {
        return Pattern.compile(CAMEL_CASE_PATTERN_NUMBERS).matcher(camelCaseString);
    }

    private void reset() {
        humanisedString = "";
        acronym = "";
    }

    private void addToAcronym(String word) {
        acronym += word;
    }

    private void appendWord(String word) {
        boolean firstWord = humanisedString.length() == 0;
        humanisedString += firstWord ? capitaliseFirstLetter(word) : " " + word.toLowerCase();
    }
    
    private void appendNumber(String number) {
        humanisedString += number;
    }

    private void appendAcronymIfThereIsOne() {
        if(acronym.length() > 0) {
            boolean firstWord = humanisedString.length() == 0;
            humanisedString += firstWord ? acronym : " " + acronym;
            acronym = "";
        }
    }

    private String capitaliseFirstLetter(String str) {
        return str.substring(0, 1).toUpperCase() + str.substring(1);
    }
}