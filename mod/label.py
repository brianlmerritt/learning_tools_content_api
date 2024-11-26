from .moodle_mod_helper import ModuleHelper


class mod_label:
    def __init__(self, moodle_rest) -> None:
        self.helper = ModuleHelper(moodle_rest, modtype='label', component_name='component', content_field='description', has_subcomponents=False)
        
    def get_label_content(self, course_modules, course):
        label_modules = course_modules[(course_modules['modname'] == 'label')]
        return self.helper.get_mod_content(label_modules, course)