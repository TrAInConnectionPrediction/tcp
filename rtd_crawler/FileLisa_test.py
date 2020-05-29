import unittest
from helpers import FileLisa
import lxml.etree as etree


class TestFileLisa(unittest.TestCase):
    test_changes_xml = """
<timetable station="Aachen Hbf" eva="8000001">

<s id="-64310431491276391-2005091918-30" eva="8000001">
    <ar cpth="Essen Hbf|Mülheim(Ruhr)Hbf|Mülheim(Ruhr)Styrum|Duisburg Hbf|Duisburg-Hochfeld Süd|Rheinhausen Ost|Rheinhausen|Krefeld-Hohenbudberg Chempark|Krefeld-Uerdingen|Krefeld-Linn|Krefeld-Oppum|Krefeld Hbf|Forsthaus|Anrath|Viersen|Mönchengladbach Hbf|Rheydt Hbf|Wickrath|Herrath|Erkelenz|Hückelhoven-Baal|Lindern|Geilenkirchen|Übach-Palenberg|Herzogenrath|Kohlscheid|Aachen West|Aachen Schanz" ct="2005092149" l="33">
        <m id="r66533843" t="f" c="0" ts="2005092050"/>
        <m id="r66533860" t="d" c="2" ts="2005092050"/>
        <m id="r66533953" t="f" c="0" ts="2005092055"/>
        <m id="r66534280" t="d" c="2" ts="2005092112"/>
    </ar>
</s>

</timetable>

"""

    concatted_changes_xml = """<timetable station="Aachen Hbf" eva="8000001">

<s id="-64310431491276391-2005091918-30" eva="8000001">
    <ar cpth="Essen Hbf|Mülheim(Ruhr)Hbf|Mülheim(Ruhr)Styrum|Duisburg Hbf|Duisburg-Hochfeld Süd|Rheinhausen Ost|Rheinhausen|Krefeld-Hohenbudberg Chempark|Krefeld-Uerdingen|Krefeld-Linn|Krefeld-Oppum|Krefeld Hbf|Forsthaus|Anrath|Viersen|Mönchengladbach Hbf|Rheydt Hbf|Wickrath|Herrath|Erkelenz|Hückelhoven-Baal|Lindern|Geilenkirchen|Übach-Palenberg|Herzogenrath|Kohlscheid|Aachen West|Aachen Schanz" ct="2005092149" l="33">
        <m id="r66533843" t="f" c="0" ts="2005092050"/>
        <m id="r66533860" t="d" c="2" ts="2005092050"/>
        <m id="r66533953" t="f" c="0" ts="2005092055"/>
        <m id="r66534280" t="d" c="2" ts="2005092112"/>
    </ar>
</s>

<s id="-64310431491276391-2005091918-30" eva="8000001">
    <ar cpth="Essen Hbf|Mülheim(Ruhr)Hbf|Mülheim(Ruhr)Styrum|Duisburg Hbf|Duisburg-Hochfeld Süd|Rheinhausen Ost|Rheinhausen|Krefeld-Hohenbudberg Chempark|Krefeld-Uerdingen|Krefeld-Linn|Krefeld-Oppum|Krefeld Hbf|Forsthaus|Anrath|Viersen|Mönchengladbach Hbf|Rheydt Hbf|Wickrath|Herrath|Erkelenz|Hückelhoven-Baal|Lindern|Geilenkirchen|Übach-Palenberg|Herzogenrath|Kohlscheid|Aachen West|Aachen Schanz" ct="2005092149" l="33">
        <m id="r66533843" t="f" c="0" ts="2005092050"/>
        <m id="r66533860" t="d" c="2" ts="2005092050"/>
        <m id="r66533953" t="f" c="0" ts="2005092055"/>
        <m id="r66534280" t="d" c="2" ts="2005092112"/>
    </ar>
</s>

</timetable>"""


    def setUp(self):
        self.fl = FileLisa()
        self.fl.delete_xml('test bhf', 0, 'test_change')


    def test_save_open(self):
        self.fl.save_station_xml(self.test_changes_xml, 'test bhf', 0, 'test_change')
        self.assertEqual(etree.tostring(self.fl.open_station_xml('test bhf', 0, 'test_change')), self.test_changes_xml)


    def test_concat(self):
        xml = self.fl.concat_xmls(etree.fromstring(self.test_changes_xml),
                                  etree.fromstring(self.test_changes_xml))
        print(etree.tostring(xml).decode("utf-8"))
        print(self.concatted_changes_xml.decode("utf-8"))
        self.assertEqual(etree.tostring(xml), self.concatted_changes_xml)

    
    def test_clean_station_name(self):
        fl = FileLisa()
        self.assertEqual(fl.clean_station_name(' Bahnhof1 / Bahnhof2'), 'Bahnhof1 slash Bahnhof2')
        self.assertEqual(fl.clean_station_name('Bahnhof'), 'Bahnhof')
        self.assertEqual(fl.clean_station_name(' Bahnhof '), 'Bahnhof')

if __name__ == "__main__":
    unittest.main()