# Erstellung-eines-umfassenden-Wetterdatensatzes-mit-st-ndlicher-H-ufigkeit
Die stündlichen Wetterdaten des Deutschen Wetterdienstes (DWD) sind pro Kategorie 
auf der Homepage als zip-Datei zur Verfügung. Es gibt 2 Dateien pro Kategorie und Messstation (eine mit historischen und eine mit aktuellen Daten).   
https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/
Das Programm kann über die Konsole gestartet und gesteuert werden.
    
Methoden:
 
 list_zipfiles(list_of_urls, file_extension)
            Extrahiert alle Links der gewünschten Wetterkategorien.
        
 filter_zipfiles()
            Filtert die Wetterstationen aus, in denen NICHT die gewünschten Wetterkategorien und 
            deren historischen und aktuellen Datensatz zur Verfügung stehen.
            
 unpack_zipfiles(path_download_folder, exlucde_filter)
            Laden Sie die Dateien temporär herunter und entpacken Sie nur den Datenbereich.
            
 import_weather_data(path)
            Lädt alle Datensätze in den Speicher und benennt sie automatisch, z.B. rain_hist_00161 
            für die historischen Werte für Regen der Station 00161. Jeder Datenframe wird als 
            einzelner Eintrag in einem Dictionary gespeichert.
            
 create_city_dict()
            Erstellen ein Dictionary mit Wetterstations-ID und den Städtenamen.
        
 merge_weather_data(data_to_merge)
            Vertikales Zusammenführen des historischen und aktuellen Datenframes pro Kategorie und pro Station. 
            Diese werden dann horizontal zu einem df pro Stadt zusammengeführt.
            
 clean_weather_data(data_to_clean)
            Formatiert und benennt Spalten um. Erzeugt ein Datetime-Objekt als Index.
            
 interpolate_weather_data(false_values)
            Fehlende Werte, die als -999 gekennzeichnet sind, werden interpoliert.
            Die Datensätze bestehen aus stündlichen Daten, aber die Frequenz ist nicht stündlich, 
            weil es an jedem Tag fehlende Beobachtungen gibt. Diese werden ebenfalls interpoliert.
    
 filter_weather_data(start_date, end_date)
            Filtert die Wetterstationen aus, die NICHT im gewünschten Zeitraum liegen
    
 plot_weather_data(data_to_plot)
            Darstellung der Wetterdaten mit einem horizontalen Balkendiagramm nach Kategorien gruppiert, einem xLabel: Datum, yLabel: Kategorie und einen Titel.
