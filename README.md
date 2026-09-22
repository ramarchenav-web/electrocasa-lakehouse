# Proyecto integrador - electrocasa-lakehouse
 By Randy Alexis Marchena Vilchez
___
## 00_setup  
#### Catalogo y Esquemas  
Para la creacion de los esquemas y del catalogo se crearan de forma automatica con el codigo indicado en caso de no existir con:  
`CREATE <Elemento> IF NOT EXISTS`  

#### Grupos y Privilegios,
Se deben tener creados los siguientes
- ingenieria
- analistas
- auditoria  

Debido al uso de Databricks Free Edition,
los grupos deben crearse manualmente asegurandonos que
esten sincronizados con nuestra cuenta
Para verificar los grupos, correr en un notebook lo siguiente:  
`%sql`  
`SHOW GROUPS;`

**Advertencia**  
Si se utiliza codigo en la creacion de los grupos,
estos no pueden ser creados a nivel 
workspace dado que no es compatible con Unity Catalog.  
En un entorno productivo los grupos deberían
ser account-level groups gestionados mediante
Unity Catalog.  

Para verificar que los permisos se concedireron adecuadamente, usar siguiente:  
`%sql`  
`SHOW GRANTS ON SCHEMA electrocasa.<Esquema>;`  
<!-- `SHOW GRANTS ON VOLUME electrocasa.bronze.vol_landing;`   -->
En caso de necesitar eliminar toda la configuracion, usar el siguiente codigo  
`%sql`  
`DROP CATALOG electrocasa CASCADE;`   
___
