/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nutch.parsefilter.combined;

import java.lang.invoke.MethodHandles;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.FileReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.HTMLMetaTags;
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.HtmlParseFilter;
import org.apache.nutch.parse.ParseResult;
import org.apache.nutch.plugin.Extension;
import org.apache.nutch.plugin.PluginRepository;
import org.apache.nutch.protocol.Content;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.*;

/**
 * RegexParseExtract. If a regular expression matches either HTML or 
 * extracted text, a configurable field is set to true.
 */
public class RegexParseExtract implements HtmlParseFilter {

  private static final Logger LOG = LoggerFactory
      .getLogger(MethodHandles.lookup().lookupClass());
  private String regexFile = null;
  
  private Configuration conf;
  private DocumentFragment doc;
  
  private static final Map<String,RegexRule> rules = new HashMap<String,RegexRule>();
  private static final Map<String,RegexRule> replaceRules = new HashMap<String,RegexRule>();
  private static final Map<String,String[]> replaceTerms = new HashMap<String,String[]>();

  
  public RegexParseExtract() {}
  
  public RegexParseExtract(String regexFile) {
    this.regexFile = regexFile;
  }

  public ParseResult filter(Content content, ParseResult parseResult, HTMLMetaTags metaTags, DocumentFragment doc) {
	  
    parseRegex(rules, content, parseResult, false);
    parseRegex(replaceRules, content, parseResult, true);
    
    return parseResult;
  }
  
  public void setConf(Configuration conf) {
    this.conf = conf;

    // domain file and attribute "file" take precedence if defined
    String file = conf.get("parsefilter.regex.file");
    String stringRules = conf.get("parsefilter.regex.rules");
    String stringReplaceRules = conf.get("parsefilter.replace.rules");

    if (regexFile != null) {
      file = regexFile;
    }
    Reader reader = null;
    if (stringRules != null) { // takes precedence over files
      reader = new StringReader(stringRules);
    } else if (file != null) {
      reader = conf.getConfResourceAsReader(file);
    }
    try {
      if (reader == null) {
        reader = new FileReader(file);
      }
      readConfiguration(reader);
    }
    catch (IOException e) {
      LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
    
    if (stringReplaceRules != null) { // takes precedence over files
    	reader = new StringReader(stringReplaceRules);
    }
    try {
    	readReplaceConfiguration(reader);
    }
    catch (IOException e) {
    	LOG.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
    }
  }
  

  public Configuration getConf() {
    return this.conf;
  }
  
  private void parseRegex(Map<String,RegexRule> rules, Content content, ParseResult parseResult, boolean replace) {
	  Parse parse = parseResult.get(content.getUrl());
	  String html = new String(content.getContent());
	  String text = parse.getText();
	  
	  for (Map.Entry<String, RegexRule> entry : rules.entrySet()) {
		  String field = entry.getKey();
		  RegexRule regexRule = entry.getValue();

		  String source = null;
		  if (regexRule.source.equalsIgnoreCase("html")) {
			  source = html;
		  }
		  if (regexRule.source.equalsIgnoreCase("text")) {
			  source = text;
		  }

		  if (source == null) {
			  LOG.error("source for regex rule: " + field + " misconfigured");
		  }

		  if (regexRule.regex != null){
			  String match = matchedString(source, regexRule.regex, replace, field);
			  // System.out.println(regexRule.regex + " " + field + " " + match);
			  
			  if (match != null && match != "") {				  
				  parse.getData().getParseMeta().set(field, match);
			  }
		  }
	  }  
  }
  
  private boolean matches(String value, Pattern pattern) {
    if (value != null) {
      Matcher matcher = pattern.matcher(value);
      return matcher.find();
    }
       
    return false;
  }
  
  private String matchedString(String value, Pattern pattern, boolean replace, String field) {
	  HashSet<String> buf = new HashSet<String>();

	  if (value != null) {
		  Matcher matcher = pattern.matcher(value);
		  		  		  	  
		  while (matcher.find()) {
			  //System.out.println("Found a " + matcher.group() + ".");			  
			  //System.out.println(matcher.groupCount());
			  String match = matcher.group();
			  
			  if (replace) {
				  String[] terms = replaceTerms.get(field);

				  for (int i = 0; i < terms.length; i++){
					  match = match.replaceAll(Pattern.quote(matcher.group(i+1)), terms[i]);
				  }
			  }
			  
			  buf.add(match);
		  }
	  }

	  return String.join(";", buf.toArray(new String[0]));
  }
  
  private synchronized void readConfiguration(Reader configReader) throws IOException {
    if (rules.size() > 0) {
      return;
    }

    String line;
    BufferedReader reader = new BufferedReader(configReader);
    while ((line = reader.readLine()) != null) {
      if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
        line = line.trim();
        String[] parts = line.split("\\s");

        if (parts.length == 3) {
            String field = parts[0].trim();
            String source = parts[1].trim();
            String regex = parts[2].trim();
            
            rules.put(field, new RegexRule(source, regex));
        } else {
            LOG.info("RegexParseExtract rule is invalid. " + line);
        }
      }
    }
  }
  
  private synchronized void readReplaceConfiguration(Reader configReader) throws IOException {
	  String line;
	  BufferedReader reader = new BufferedReader(configReader);

	  while ((line = reader.readLine()) != null) {
		  if (StringUtils.isNotBlank(line) && !line.startsWith("#")) {
			  line = line.trim();
			  String[] parts = line.split("\\s");

			  if (parts.length >= 3) {
				  String field = parts[0].trim();
				  String source = parts[1].trim();
				  String regex = parts[2].trim();
				  String[] terms = Arrays.copyOfRange(parts, 3, parts.length);

				  replaceRules.put(field, new RegexRule(source, regex));
				  replaceTerms.put(field, terms);
				  
				  //System.out.println("terms " + Arrays.toString(terms));
			  } else {
				  LOG.info("RegexParseExtract rule is invalid. " + line);
			  }
		  }
	  }
  }
  
  private static class RegexRule {
    public RegexRule(String source, String regex) {
      this.source = source;
      this.regex = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    }
    String source;
    Pattern regex;
  }
}