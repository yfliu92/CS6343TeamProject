package com.google.code.gossip.manager.Ring;
import java.util.*;

public class Command {

	String rawCommand;
	String action;
	String input;
	String node1;
	String node2;
	List<String> series;
	
	public Command(String commandStr){
		this.rawCommand = commandStr;
		String[] series = commandStr.split(" ");
		if (series.length > 0) {
			this.action = series[0];
			if (series.length == 2) {
				this.input = series[1];
			}
			else if (series.length == 3) {
				this.node1 = series[1];
				this.node2 = series[2];
			}
			this.series = getCommandSeries(series);
		}
	}
	
	public static List<String> getCommandSeries(String[] series) {
		List<String> result = new ArrayList<>();
		if (series.length > 1) {
			for(int i = 1; i < series.length; i++) {
				result.add(series[i]);
			}
		}
		
		return result;
	}
	
	public List<String> getCommandSeries() {
		return this.series;
	}
	
	public static Command getCommand(String commandStr) {
		
//		String[] series = commandStr.split(" ");
		return new Command(commandStr);
//		if (series[0].equals("find")) {
//			command = new Command("find", series[1]);
//			command.series = getCommandSeries(series);
//			return new Command("find", series[1]);
//		}
//		else if (series[0].equals("loadbalance")) {
//			return new Command("loadbalance", series[1], series[2]);
//		}
//		else if (series[0].equals("add")) {
//			return new Command("add", series[1]);
//		}
//		else if (series[0].equals("remove")) {
//			return new Command("remove", series[1]);
//		}
//		else {
//			return null;
//		}
	}
	
	public String getAction() {
		return this.action;
	}
	
	public String getInput() {
		return this.input;
	}
	
	public List<String> getContent() {
		return this.series;
	}
	
	public String getRawCommand() {
		return this.rawCommand;
	}
}
