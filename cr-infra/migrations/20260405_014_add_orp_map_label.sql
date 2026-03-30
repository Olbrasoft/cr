-- Add map_label column for shortened ORP names on SVG maps
ALTER TABLE orp ADD COLUMN map_label TEXT;

-- Populate shortened labels based on reference maps
-- Středočeský kraj
UPDATE orp SET map_label = 'Brandýs' WHERE name = 'Brandýs nad Labem-Stará Boleslav';
UPDATE orp SET map_label = 'Ml. Boleslav' WHERE name = 'Mladá Boleslav';
UPDATE orp SET map_label = 'Mnich. Hradiště' WHERE name = 'Mnichovo Hradiště';
UPDATE orp SET map_label = 'Kralupy' WHERE name = 'Kralupy nad Vltavou';
UPDATE orp SET map_label = 'Lysá' WHERE name = 'Lysá nad Labem';
UPDATE orp SET map_label = 'Říčany' WHERE name = 'Říčany';
UPDATE orp SET map_label = 'Černošice' WHERE name = 'Černošice';

-- Jihočeský kraj
UPDATE orp SET map_label = 'Týn' WHERE name = 'Týn nad Vltavou';
UPDATE orp SET map_label = 'Jindřichův Hradec' WHERE name = 'Jindřichův Hradec';

-- Jihomoravský kraj
UPDATE orp SET map_label = 'Slavkov' WHERE name = 'Slavkov u Brna';
UPDATE orp SET map_label = 'Veselí' WHERE name = 'Veselí nad Moravou';
UPDATE orp SET map_label = 'Mor. Krumlov' WHERE name = 'Moravský Krumlov';

-- Královéhradecký kraj
UPDATE orp SET map_label = 'Dvůr Králové' WHERE name = 'Dvůr Králové nad Labem';
UPDATE orp SET map_label = 'Nové Město' WHERE name = 'Nové Město nad Metují';
UPDATE orp SET map_label = 'Kostelec n.O.' WHERE name = 'Kostelec nad Orlicí';
UPDATE orp SET map_label = 'Rychnov' WHERE name = 'Rychnov nad Kněžnou';

-- Liberecký kraj
UPDATE orp SET map_label = 'Jablonec' WHERE name = 'Jablonec nad Nisou';

-- Moravskoslezský kraj
UPDATE orp SET map_label = 'Frenštát' WHERE name = 'Frenštát pod Radhoštěm';
UPDATE orp SET map_label = 'Frýdlant' WHERE name = 'Frýdlant nad Ostravicí';

-- Olomoucký kraj
UPDATE orp SET map_label = 'Lipník' WHERE name = 'Lipník nad Bečvou';

-- Pardubický kraj
UPDATE orp SET map_label = 'Mor. Třebová' WHERE name = 'Moravská Třebová';

-- Ústecký kraj
UPDATE orp SET map_label = 'Ústí n.L.' WHERE name = 'Ústí nad Labem';
UPDATE orp SET map_label = 'Roudnice' WHERE name = 'Roudnice nad Labem';

-- Vysočina
UPDATE orp SET map_label = 'Světlá n.S.' WHERE name = 'Světlá nad Sázavou';
UPDATE orp SET map_label = 'Mor. Budějovice' WHERE name = 'Moravské Budějovice';
UPDATE orp SET map_label = 'Náměšť n.O.' WHERE name = 'Náměšť nad Oslavou';
UPDATE orp SET map_label = 'Nové Město' WHERE name = 'Nové Město na Moravě';
UPDATE orp SET map_label = 'Bystřice' WHERE name = 'Bystřice nad Pernštejnem';
UPDATE orp SET map_label = 'Žďár n.S.' WHERE name = 'Žďár nad Sázavou';

-- Zlínský kraj
UPDATE orp SET map_label = 'Bystřice p.H.' WHERE name = 'Bystřice pod Hostýnem';
UPDATE orp SET map_label = 'Rožnov p.R.' WHERE name = 'Rožnov pod Radhoštěm';
