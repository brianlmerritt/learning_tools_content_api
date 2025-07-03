from .moodle_mod_helper import ModuleHelper


class mod_book:
    def __init__(self, moodle_rest) -> None:
        self.mod_helper = ModuleHelper(moodle_rest, modtype='book', component_name='chapter', content_field='contents', has_subcomponents=True)
        
    def get_book_content(self, course_modules, course):
        book_modules = course_modules[(course_modules['modname'] == 'book')]
        return self.mod_helper.get_mod_content(book_modules, course)
    
    def delete_book_file(self, book_cmid, book_chapter_id, filename):
        raise NotImplementedError("Deleting book files is not supported in this version of the Moodle API.")