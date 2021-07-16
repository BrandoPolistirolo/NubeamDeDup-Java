# NubeamJava
Uses FastDoop Library ---- https://github.com/umbfer/fastdoop/tree/spark_support (See Repository for install instructions)

# Nubeam De-Duplication Java
il progetto è un'implementazione del paper di *Hang Dai* e *Yongtao Guan* intitolato : *Nubeam-dedup: a fast and RAM-efficient tool to
de-duplicate sequencing reads without mapping*.

------------


L'algoritmo di de-duplicazione si basa sulla trasformazione di una sequenza genetica nel suo corrispettivo numero *NuBeam*. Qualsiasi sequenza genetica può essere rappresentata tramite un numero NuBeam, tale numero viene ricavato attraverso due passi :
- Trasformare la sequenza in sequenza binaria : data una sequenza di lunghezza L la trasformazione restituisce una sequenza binaria di lunghezza 4*L. La sequenza binaria contiene nelle prime L posizioni una combinazione di 1 e 0, dove una posizione è uguale a 1 se nella sequenza originale compare A e 0 altrimenti. Lo stesso procedimento si ripete per le prossime L posizioni della sequenza binaria, tuttavia per la base T e così via fino ad arrivare all'ultima base G. Ad esempio la sequenza ATCAGC diventa la seguente : 100100 010000 001001 000010.
- Nel secondo passo viene utilizzata una notazione matriciale per rappresentare gli 1 e 0 della sequenza : M1 = [1,1][0,1] e M0 = [1,0][1,1] , in questo si può ottenere per ogni sequenza il prodotto matriciale MB= Produttoria di M_i,  dove M_i è M1 o M0 a seconda della presenza di 1 o 0 nella posizione i della sequenza. Dopodichè il numero Nubeam viene calcolato facendo la traccia della matrice MB*W. W è una matrice dei pesi definita come W = [1 , sqrt(3)] [sqrt(2) , sqrt(5)]. Dunque NuBeam = Tr(MB*W). La figura a) sottostante mostra il procedimento per ottenere il numero NuBeam.


![Origine : Pubblicazione NuBeam](https://oup.silverchair-cdn.com/oup/backfile/Content_public/Journal/bioinformatics/36/10/10.1093_bioinformatics_btaa112/2/m_btaa112f1.jpeg?Expires=1629501309&Signature=xe6TxW39cJUOSta8XCQhvbX-mrxvt2Eox2vSPl8APb8l8fwKRjPVexb4DFJgIBCGXZpg7es2sXujXC-IqsC0WY8-d9Feu5lQ8sedC2mmyGKDnMbXbEYx~eTaLOsZv5OHAJ4vAFFQaDlLcsO16rFj~Aku4Jp9S9cKjZ9dlO20aYsDKKxelMIUZ6GpWOY9gpE6GVWuy-JNyuH5Teu6MyKd1K8KQArO2gGzYceoLayNzFuTJnYwSRIjENNyHjJbGXYSZg4btkFqX1qo4s3BD9YfHPXCVVqSOlTaVppexQkWysFqEvyVuuwBOOJCG36C9vodTdk13WANM1Iuf8k31GcLvQ__&Key-Pair-Id=APKAIE5G5CRDK6RD3PGA "Origine : Pubblicazione NuBeam")


------------

# Il Caso Single-End
L'algoritmo dunque sfrutta il fatto che due sequenze diverse non possono avere lo stesso numero NuBeam e che due sequenze uguali devono necessariamente avere lo stesso numero NuBeam. L'algoritmo si divide in due casi a seconda del tipo di letture genetiche che si vogliono de-duplicare. 
Il primo caso sono le Single End Reads ovvero le letture che vengono fatte da un'estremità all'altra della sequenza. L'algoritmo per le SE è descritto in breve nei seguenti passi :
1.  Viene creato un *unordered_set* dove all'interno ci saranno i numeri NuBeam delle sequenze già processate memorizzati come chiavi(keys). 
2. Per ogni nuova sequenza analizzata si controlla se il suo numero NuBeam appartiene già alla lista : se vi appartiene allora si passa alla sequenza successiva, se non vi appartiene si controlla se il suo Complementare inverso appartenga alla lista(sempre tramite numero NuBeam). Nel caso entrambi i controlli diano risposta negativa la sequenza viene aggiunta alla lista.
Il complementare inverso viene ottenuto prima invertendo la sequenza e poi calcolando il complementare, ovvero cambiando da A a T e viceversa e da C a G e viceversa. 

------------

## La Nostra Implementazione
Le sequenze vengono importate dal file FASTQ in un PairRDD grazie alla libreria Fastdoop. Il PairRDD contiene Stringhe per l'id della sequenza e un'altra stringa per la sequenza stessa. Dopo esser stato importato il file, si procede subito con l'algoritmo single end. La nostra implementazione prevede una funzione specifica descritta qui sotto : 
```java
public static List<Long> NubeamSE(JavaPairRDD<String,String> x ) {
		// Hash Map dove memorizziamo le coppie Nubeam-indice già processate
		HashMap<Double,Long> tempmap = new HashMap<>();
		// Lista dove verranno memorizzati gli indici delle sequenze uniche
		List<Long> indexes = new ArrayList<>();
		long index = 0;
		// reversed index serve per identificare i numeri Nubeam derivanti
		
		// dall'operazione complementare inverso. (poichè vengono aggiunti se la 
		// sequenza non è un duplicato ma vanno poi tolti in seguito)
		long reversedindex = -1;
		//ciclo for che itera il PairRDD
		for (Tuple2<String,String> sequence : x.collect()) {
		// il numero nubeam viene calcolato con funzioni dedicate interne alla classe Generic
			double Nubeam = Generic.BinaryToNubeam(Generic.serializza(sequence._2()));
			if (tempmap.containsKey(Nubeam)) {
				continue;
			}
			tempmap.put(Nubeam, index);
			String ReversedSequence = Generic.reverseString(sequence._2());
			double NubeamReversedComplement = Generic.BinaryToNubeam(Generic.serializza(ReversedSequence));
			tempmap.put(NubeamReversedComplement,reversedindex);
			index = index + 1;
		}
		indexes.addAll(tempmap.values());
		//togli le chiavi con indice uguale a -1
		indexes.removeIf(n ->  (n == -1) );
		return indexes;
		// la funzione restituisce un indice che poi verrà utilizzato per filtrare
		// il pairRDD originale per ricavare un PairRDD con le sole sequenze 
		// uniche, questo procedimento ci permette di poter salvare su file sia 
		// le sequenze uniche ma anche quelle rimosse (tramite un 'filtro inverso')
	}
```
La lista degli indici viene quindi utilizzata per filtrare l'RDD originale tramite la funzione .filter di Spark. 
