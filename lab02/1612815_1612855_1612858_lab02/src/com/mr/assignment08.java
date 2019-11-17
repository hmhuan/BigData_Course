package com.mr;

public class assignment08 {
	public static void main(String[] args) throws Exception {
		
		 if (args[0].contentEquals("UniqueListener")) 		
		 { 
			 // task 01: Number of unique listeners 
			 UniqueListener uniqueListener = new UniqueListener(); 
			 uniqueListener.run(args); 
		 } 
		 else if (args[0].contentEquals("SharedTrack"))  	
		 {
			 // task 02: Number of times the track was shared with others
			 SharedTrack sharedTrack = new SharedTrack();
			 sharedTrack.run(args);
		 }
		 else if(args[0].contentEquals("ListenedTrack")) 	
		 { 
			 // task 03: Number of times the track was listened to on the radio	
			 ListenedTrack listenedTrack = new ListenedTrack(); 
			 listenedTrack.run(args); 
		 }
		 else if (args[0].contentEquals("ListenedTotalTrack"))
		 {
			 // task 04: Number of times the track was listened to in total (skip = 0)
			 ListenedTotalTrack listenedTotalTrack = new ListenedTotalTrack();
			 listenedTotalTrack.run(args);
		 }
		 else if (args[0].contentEquals("SkippedOnRadioTrack"))	
		 {
			 // task 05: Number of times the track was skipped on the radio
			 SkippedOnRadioTrack skippedOnRadioTrack = new SkippedOnRadioTrack();
			 skippedOnRadioTrack.run(args);
		 }
	}
}
