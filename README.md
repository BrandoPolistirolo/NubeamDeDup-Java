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
La lista degli indici viene quindi utilizzata per filtrare l'RDD originale tramite la funzione .filter di Spark, gli indici vengono poi eliminati tramite la funzione .MapToPair ottenendo così un RDD composto dalla Tuple2 id-Sequenze pronto per essere salvato in formato FASTQ.  
# Il Caso Paired-End
l'algoritmo per quanto riguarda i dati sequenziati con il metodo paired-end cambia rispetto al single-end in quanto bisognerà far funzionare l'algoritmo su due file anzichè uno.
Per il caso Paired-end l'algoritmo è il seguente (descritto in breve) :
- Vengono creati due 'contenitori' una Multimap(i1,i) ed una HashMap(i,i2), dove i1 e i2 sono rispettivamente i numeri Nubeam per la lettura uno e per la lettura due.
i è l'indice di riga. 
- l'algoritmo scorre le righe e analizza se la coppia di lettura letta (j1,j2) è un duplicato, questo avviene se si verificano due condizioni la cui seconda può verificarsi solo se la prima è vera : 1) Se la coppia di lettura è un duplicato allora il primo contenitore (ovvero la multimap) contiene il numero nubeam j1. 2) tra gli indici che legano il numero j1 (più precisamente la sua copia già contenuta nella multimap) ve ne sono alcuni nella HashMap che hanno come valore il numero nubeam j2. Se entrambe sono vere allora la coppia è un duplicato. La coppia viene quindi aggiunta alla Multimap e alla HashMap se una delle due condizioni è falsa.
- L'algoritmo prende in considerazione i complementari calcolando per ogni coppia la sua rispettiva coppia complementare e di conseguenza anche i suoi numeri nubeam. Prima si controlla se la coppia originale è un duplicato, dopodichè si controlla se la coppia complementare è un duplicato, se nessuna delle due sono duplicati allora si aggiunge la coppia originale nei rispettivi 'contenitori'.

------------


## Implementazione 
Le sequenze vengono importate dal file FASTQ e immagazzinate in due PairRDD separati grazie alla libreria fastdoop. I due RDD devono poi essere 'zipped' affinchè l'algoritmo possa scorrere entrambi i file contemporaneamente.
Viene quindi utilizzata la funzione .zip dopo aver ridotto i due RDD ad una sola partizione con la funzione .coalesce(1).
La funzione Paired-End è commentata in seguito :
```java
// la funzione prende come argomento il PairRDD 'combinato'
public static List<Long> NubeamPE (JavaPairRDD<Tuple2<String,String>,Tuple2<String,String>> x) {
		// Multimap nubeam - index
		Multimap<Double, Long> map = ArrayListMultimap.create();
		// Hashmap index - nubeam
		HashMap<Long,Double> map2 = new HashMap<>();
		//lista dove andremo a salvare gli indici delle coppie
		List<Long> res = new ArrayList<>();
		long index = 0;
		for ( Tuple2<Tuple2<String,String>,Tuple2<String,String>> id_sequence : x.collect() ) {
			//leggi le sequenze e calcola i nubeam
			double Nubeam1 = Generic.BinaryToNubeam(Generic.serializza(id_sequence._1._2()));
			double Nubeam2 = Generic.BinaryToNubeam(Generic.serializza(id_sequence._2._2()));
			int check1 = 0;
			//controllo duplicati 
			if (map.containsKey(Nubeam1)) {
			//se la prima condizione si verifica itera tra gli indici
			//della multimap legati al nubeam1
				for(Long ind : map.get(Nubeam1)) {
				//se tra questi indici ve ne è uno tale che il valore corrispondente
				// è uguale a nubeam2 allora la coppia di lettura è un duplicato
					if(map2.get(ind).equals(Nubeam2)) {
						check1 = 1;
					}
				}
			}
			if (check1 == 1) continue;
			
			// controlla se la coppia complementare è un duplicato
			double Nubeam1Complementary = Generic.BinaryToNubeam(Generic.serializza(Generic.Complementare(id_sequence._1._2())));
			double Nubeam2Complementary = Generic.BinaryToNubeam(Generic.serializza(Generic.Complementare(id_sequence._2._2())));
			int check2 = 0;
			// la stessa logica di prima , solamente applicata alla coppia complementare
			if(map.containsKey(Nubeam1Complementary)) {
				for(Long ind : map.get(Nubeam1Complementary)) {
					if(map2.get(ind).equals(Nubeam2Complementary)) {
						check2 = 1;
					}
				}
			}
			if (check2 == 1) continue;
			//la funzione salta l'iterazione se la coppia originale è duplicata 
			// oppure se la coppia complementare è duplicata
			//inserisci i dati nella multimap e nella hashmap
			map.put(Nubeam1, index);
			map2.put(index, Nubeam2);
			index += 1;
		}
		//trasferisci gli indici contenuti nella multimap in una lista
		res.addAll(map.values());
		map.clear();
		map2.clear();
		//restituisci la lista
		return res;
```

La lista degli indici viene poi utilizzata per filtrare entrambi i PairRDD originali ottenendo così RDD con le sole sequenze de-duplicate.
## Opzioni del Programma
Il programma interroga l'utente prima di iniziare il procedimento di de-duplicazione con i seguenti quesiti:
- Scegliere il tipo di lettura : Single-End o Paired-End
- inserire il nome del file in formato FASTQ (o dei file nel caso Paired-end)
- Se vuole salvare su file le sequenze de-duplicate (le sequenze vengono salvate su un file fastq, depurate dai dati riguardanti la qualità del sequenziamento)
- Se vuole salvare su file le sequenze rimosse
- Se vuole far girare il programma su un campione dei dati (in caso di risposta affermativa il programma chiede un numero da 1 a 100 rappresentante la percentuale che l'utente vuole campionare).
- il programma infine restituisce alcune informazioni riguardanti il processo di de-duplicazione (numero di sequenze rimosse, numero di sequenze de-duplicate sul totale e tempo impiegato dal programma espresso in secondi)
# Risultati
Abbiamo testato il programma su tre campioni diversi dei dati originali (troppo grandi per essere processati) : 
- un campione 'Small' da 750 000  coppie di sequenze 
- un campione 'Medium' da 2 250 000 coppie di sequenze
- un campione 'Large' da 5 000 000 coppie di sequenze
L'algoritmo era stato precedentemente testato sui toydata forniti dalla pagina github dei ricercatori ottenendo risultati analoghi ai loro. 
## Tabella Risultati
**| | Small  | Medium  | Large  |
| ------------ | ------------ | ------------ | ------------ |
| Sequenze Originali  | 750 000 x2 | 2 250 000 x2 | 5 000 000 x2 |
| Sequenze Deduplicate | 748 652 | 2 245 655 | 4 990 178 |
| Sequenze Rimosse  | 1348 | 4345 | 9822 |
| Tempo (secondi) | 53 | 169 | 341 |
| Committed Memory (Kbytes) | 2 760 704 | 4 931 584 | 11 771 904 |**
