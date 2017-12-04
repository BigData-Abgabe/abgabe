# Aufgabe 1

## Map
Ist kein Monoidhomomorphismus, da hier keine Assoziativität vorliegt

## Reduce
Ist ein Monoidhomomorphismus, da der Key einfach weitergereicht wird und der Value lediglich eine Summe der Eingabewerte ist. Summen sind Monoidhomomorphismen, daher ist es auch dieser Reducer. 

## MapReduce
Ist kein Monoidhomomorphismus, da hier aufgrund der verschiedenen Eingabe- und Ausgabetypen keine Assoziativität vorhanden ist. 
